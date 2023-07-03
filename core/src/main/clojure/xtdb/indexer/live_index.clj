(ns xtdb.indexer.live-index
  (:require [juxt.clojars-mirrors.integrant.core :as ig]
            xtdb.buffer-pool
            xtdb.object-store
            [xtdb.types :as types]
            [xtdb.util :as util]
            [xtdb.vector.writer :as vw])
  (:import (java.lang AutoCloseable)
           (java.util Arrays HashMap Map)
           (java.util.concurrent CompletableFuture)
           (java.util.concurrent.atomic AtomicInteger)
           (java.util.function BiConsumer BiFunction Function IntConsumer)
           (java.util.stream IntStream)
           (org.apache.arrow.memory BufferAllocator)
           (org.apache.arrow.vector VectorSchemaRoot)
           (org.apache.arrow.vector.types.pojo ArrowType$Union Schema)
           org.apache.arrow.vector.types.UnionMode
           (xtdb.object_store ObjectStore)
           (xtdb.trie HashTrie HashTrie$Visitor MemoryHashTrie TrieKeys)
           (xtdb.vector IIndirectRelation IIndirectVector IRelationWriter)))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTableTx
  (^xtdb.vector.IRelationWriter leafWriter [])
  (^void addRow [idx])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveTable
  (^xtdb.indexer.live_index.ILiveTableTx startTx [])
  (^java.util.concurrent.CompletableFuture #_<?> finishChunk [^long chunkIdx])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndexTx
  (^xtdb.indexer.live_index.ILiveTableTx liveTable [^String tableName])
  (^void commit [])
  (^void close []))

#_{:clj-kondo/ignore [:clojure-lsp/unused-public-var]}
(definterface ILiveIndex
  (^xtdb.indexer.live_index.ILiveTable liveTable [^String tableName])
  (^xtdb.indexer.live_index.ILiveIndexTx startTx [])
  (^void finishChunk [^long chunkIdx])
  (^void close []))

(def ^org.apache.arrow.vector.types.pojo.Schema trie-schema
  (Schema. [(types/->field "nodes" (ArrowType$Union. UnionMode/Dense (int-array (range 3))) false
                           (types/col-type->field "nil" :null)
                           (types/col-type->field "branch" [:list [:union #{:null :i32}]])
                           ;; TODO metadata
                           (types/col-type->field "leaf" '[:struct {page-idx :i32}]))]))

(defn- write-trie!
  ^java.util.concurrent.CompletableFuture [^BufferAllocator allocator, ^ObjectStore obj-store,
                                           ^String table-name, ^String trie-name, ^String chunk-idx,
                                           ^HashTrie static-trie, ^IIndirectRelation static-leaf]

  (util/with-close-on-catch [leaf-vsr (VectorSchemaRoot/create (Schema. (for [^IIndirectVector rdr static-leaf]
                                                                          (.getField (.getVector rdr))))
                                                               allocator)
                             trie-vsr (VectorSchemaRoot/create trie-schema allocator)]
    (let [leaf-rel-wtr (vw/root->writer leaf-vsr)
          trie-rel-wtr (vw/root->writer trie-vsr)

          node-wtr (.writerForName trie-rel-wtr "nodes")
          node-wp (.writerPosition node-wtr)

          branch-wtr (.writerForTypeId node-wtr (byte 1))
          branch-el-wtr (.listElementWriter branch-wtr)

          leaf-wtr (.writerForTypeId node-wtr (byte 2))
          page-idx-wtr (.structKeyWriter leaf-wtr "page-idx")
          !page-idx (AtomicInteger. 0)
          copier (vw/->rel-copier leaf-rel-wtr static-leaf)]

      (-> (.putObject obj-store
                      (format "tables/%s/%s/leaf-c%s.arrow" table-name trie-name chunk-idx)
                      (util/build-arrow-ipc-byte-buffer leaf-vsr :file
                        (fn [write-batch!]
                          (.accept static-trie
                                   (reify HashTrie$Visitor
                                     (visitBranch [visitor children]
                                       (let [!page-idxs (IntStream/builder)]
                                         (doseq [^HashTrie child children]
                                           (.add !page-idxs (if child
                                                              (do
                                                                (.accept child visitor)
                                                                (dec (.getPosition node-wp)))
                                                              -1)))
                                         (.startList branch-wtr)
                                         (.forEach (.build !page-idxs)
                                                   (reify IntConsumer
                                                     (accept [_ idx]
                                                       (if (= idx -1)
                                                         (.writeNull branch-el-wtr nil)
                                                         (.writeInt branch-el-wtr idx)))))
                                         (.endList branch-wtr)
                                         (.endRow trie-rel-wtr)))

                                     (visitLeaf [_ _page-idx idxs]
                                       (-> (Arrays/stream idxs)
                                           (.forEach (reify IntConsumer
                                                       (accept [_ idx]
                                                         (.copyRow copier idx)))))

                                       (.syncRowCount leaf-rel-wtr)
                                       (write-batch!)
                                       (.clear leaf-rel-wtr)
                                       (.clear leaf-vsr)

                                       (.startStruct leaf-wtr)
                                       (.writeInt page-idx-wtr (.getAndIncrement !page-idx))
                                       (.endStruct leaf-wtr)
                                       (.endRow trie-rel-wtr)))))))
          (util/then-compose
            (fn [_]
              (.syncRowCount trie-rel-wtr)
              (.putObject obj-store
                          (format "tables/%s/%s/trie-c%s.arrow" table-name trie-name chunk-idx)
                          (util/root->arrow-ipc-byte-buffer trie-vsr :file))))
          (.whenComplete (reify BiConsumer
                           (accept [_ _ _]
                             (util/try-close trie-vsr)
                             (util/try-close leaf-vsr))))))))

(defn- add-all ^xtdb.trie.MemoryHashTrie [^MemoryHashTrie trie, ^ints idxs]
  ;; TODO MHT.addAll, or maybe a specific merge operation...
  (loop [n 0, trie trie]
    (if (= n (alength idxs))
      trie
      (recur (inc n) (.add trie (aget idxs n))))))

(defrecord LiveTableTx [^IRelationWriter static-leaf, ^IRelationWriter transient-leaf
                        ^Map static-tries, ^Map transient-tries]
  ILiveTableTx
  (leafWriter [_] transient-leaf)

  (addRow [_ idx]
    (.compute transient-tries "t1-diff"
              (reify BiFunction
                (apply [_ _trie-name transient-trie]
                  (let [^MemoryHashTrie
                        transient-trie (or transient-trie
                                           (MemoryHashTrie/emptyTrie (TrieKeys. transient-leaf)))]

                    (.add transient-trie idx))))))

  (commit [_]
    (let [copier (vw/->rel-copier static-leaf (vw/rel-wtr->rdr transient-leaf))]
      (doseq [[trie-name ^MemoryHashTrie transient-trie] transient-tries]
        (.compute static-tries trie-name
                  (reify BiFunction
                    (apply [_ _trie-name static-trie]
                      (let [!new-static-trie (volatile!
                                              (or static-trie (MemoryHashTrie/emptyTrie (TrieKeys. static-leaf))))]
                        (.accept transient-trie
                                 (reify HashTrie$Visitor
                                   (visitBranch [visitor children]
                                     (run! #(.accept ^HashTrie % visitor) children))

                                   (visitLeaf [_ _page-idx idxs]
                                     (let [!static-idxs (IntStream/builder)]
                                       (-> (Arrays/stream idxs)
                                           (.forEach (reify IntConsumer
                                                       (accept [_ idx]
                                                         (.add !static-idxs (.copyRow copier idx))))))

                                       (let [static-idxs (.toArray (.build !static-idxs))]
                                         (vswap! !new-static-trie add-all static-idxs))))))
                        @!new-static-trie)))))
      (.clear transient-tries)))

  AutoCloseable
  (close [_]
    (util/close transient-leaf)))

(def ^:private ^org.apache.arrow.vector.types.pojo.Schema leaf-schema
  (Schema. [(types/col-type->field "xt$iid" [:fixed-size-binary 16])
            (types/col-type->field "xt$valid_from" types/nullable-temporal-type)
            (types/col-type->field "xt$valid_to" types/nullable-temporal-type)
            (types/col-type->field "xt$system_from" types/nullable-temporal-type)
            (types/col-type->field "xt$system_to" types/nullable-temporal-type)
            (types/col-type->field "xt$doc" [:struct {}])]))

(defn- open-leaf-root ^xtdb.vector.IRelationWriter [^BufferAllocator allocator]
  (vw/root->writer (VectorSchemaRoot/create leaf-schema allocator)))

(defrecord LiveTable [^BufferAllocator allocator, ^ObjectStore obj-store, ^String table-name
                      ^IRelationWriter static-leaf, ^Map static-tries]
  ILiveTable
  (startTx [_]
    (util/with-close-on-catch [transient-leaf (open-leaf-root allocator)]
      (LiveTableTx. static-leaf transient-leaf
                    static-tries (HashMap.))))

  (finishChunk [_ chunk-idx]
    (let [chunk-idx-str (util/->lex-hex-string chunk-idx)]
      (CompletableFuture/allOf
       (->> (for [[trie-name trie] static-tries]
              (write-trie! allocator obj-store table-name trie-name chunk-idx-str trie (vw/rel-wtr->rdr static-leaf)))
            (into-array CompletableFuture)))))

  AutoCloseable
  (close [_]
    (util/close static-leaf)))

(defrecord LiveIndex [^BufferAllocator allocator, ^ObjectStore object-store, ^Map tables]
  ILiveIndex
  (liveTable [_ table-name]
    (.computeIfAbsent tables table-name
                      (reify Function
                        (apply [_ table-name]
                          (util/with-close-on-catch [rel (open-leaf-root allocator)]
                            (LiveTable. allocator object-store table-name
                                        rel (HashMap.)))))))

  (startTx [live-idx]
    (let [table-txs (HashMap.)]
      (reify ILiveIndexTx
        (liveTable [_ table-name]
          (.computeIfAbsent table-txs table-name
                            (reify Function
                              (apply [_ table-name]
                                (-> (.liveTable live-idx table-name)
                                    (.startTx))))))

        (commit [_]
          (doseq [^ILiveTableTx table-tx (.values table-txs)]
            (.commit table-tx)))

        AutoCloseable
        (close [_]
          (util/close table-txs)))))

  (finishChunk [_ chunk-idx]
    @(CompletableFuture/allOf (->> (for [^ILiveTable table (.values tables)]
                                     (.finishChunk table chunk-idx))

                                   (into-array CompletableFuture)))

    (util/close tables)
    (.clear tables))

  AutoCloseable
  (close [_]
    (util/close tables)))

(defmethod ig/prep-key :xtdb.indexer/live-index [_ opts]
  (merge {:allocator (ig/ref :xtdb/allocator)
          :object-store (ig/ref :xtdb/object-store)}
         opts))

(defmethod ig/init-key :xtdb.indexer/live-index [_ {:keys [allocator object-store]}]
  (LiveIndex. allocator object-store (HashMap.)))

(defmethod ig/halt-key! :xtdb.indexer/live-index [_ live-idx]
  (util/close live-idx))