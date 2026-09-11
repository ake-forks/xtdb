(ns xtdb.bench.pg-source-tx-overhead
  "The IngestTsOverhead sweep run through CDC, with the writer and the mirror separated in time
   so neither measurement contends with the other.

   Per batch size: attach a source database against the empty table, close the node, write
   `doc-count` rows into Postgres as transactions of `batch-size` while nothing is consuming,
   then reopen the node and time the source draining the backlog.

   `pg-ingest-batch-N` is Postgres alone; `xt-drain-batch-N` is the source alone, and dividing
   it by `doc-count / batch-size` is the per-transaction cost the benchmark is named for.

   Both of the source database's logs go through Kafka, so the drain measures what a deployment
   would pay: every mirrored transaction is a round trip to the broker on the replica log.

   See modules/bench/README.adoc for what a run needs, and what it takes over in Postgres and Kafka.

   Run: ./gradlew pg-source-tx-overhead -PdocCount=100000 [-PbatchSizes=1000,1] [-Pyourkit]"
  (:require [clojure.string :as s]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.bench :as b]
            [xtdb.node :as xtn]
            [xtdb.util :as util])
  (:import [java.nio.file Path]
           [java.sql Connection Statement]))

(def ^:private default-pg-url "jdbc:postgresql://localhost:5433/postgres?user=postgres&password=postgres")

;; the `indexer` key arrived with 5f673a873 and the config parser rejects unknown keys, so an
;; older node needs this empty rather than defaulted
(def ^:private default-indexer "!DirectMirror {}")

;; the source applies transactions in LSN order, so the whole backlog has arrived once the row
;; written after it has
(def ^:private snapshotted-id 1)
(def ^:private streamed-id 2)
(def ^:private drained-id 3)

(def ^:private drain-poll-ms 5)
(def ^:private drain-timeout-ms (* 10 60 1000))

;; the alias the node config registers the broker under, shared with `remotes` - see
;; modules/bench/config/pg-source.yaml
(def ^:private kafka-cluster "kafka")

;; Kafka auto-creates these, and a topic that survived the run would be replayed by the next one,
;; so every run gets its own. They are left behind - `docker compose down -v kafka` clears them.
(defn- ->topic-prefix [] (str "xtdb-bench-pg-" (subs (str (random-uuid)) 0 8)))

(defn- ->names [topic-prefix ^long batch-size]
  {:db (str "bench_pg_" batch-size)
   :table (str "batched_" batch-size)
   :marker (str "marker_" batch-size)
   :slot (str "xtdb_bench_pg_" batch-size)
   :publication (str "xtdb_bench_pg_" batch-size)
   :topic (str topic-prefix "-" batch-size)

   ;; the node's own log is per-batch-size too, because it carries the ATTACH records. Shared, the
   ;; batch-1 node would replay every earlier batch size's ATTACH, re-open those databases against
   ;; a storage directory the cleanup stage has deleted, and run their sources alongside the one
   ;; being measured
   :node-topic (str topic-prefix "-" batch-size "-node")})

;; --- postgres ---

(defn- pg-conn ^Connection [pg-url]
  (jdbc/get-connection {:jdbcUrl pg-url}))

(defn- execute! [^Connection conn & sqls]
  (with-open [^Statement stmt (.createStatement conn)]
    (doseq [^String sql sqls]
      (.execute stmt sql))))

(defn- setup-pg! [pg-url {:keys [table marker slot publication]}]
  (with-open [conn (pg-conn pg-url)]
    (apply execute! conn
           (concat [(format "DROP PUBLICATION IF EXISTS %s" publication)

                    ;; a slot outlives the node that created it, so the previous run's is still
                    ;; holding WAL - reclaim it here, where nothing has it open
                    (format "SELECT pg_drop_replication_slot('%s') FROM pg_replication_slots WHERE slot_name = '%s'"
                            slot slot)]

                   (mapcat (fn [t]
                             [(format "DROP TABLE IF EXISTS %s" t)
                              (format "CREATE TABLE %s (_id BIGINT PRIMARY KEY)" t)])
                           [table marker])

                   [(format "CREATE PUBLICATION %s FOR TABLE %s, %s" publication table marker)]))))

(defn- write-marker! [pg-url {:keys [marker]} ^long id]
  (with-open [conn (pg-conn pg-url)]
    (execute! conn (format "INSERT INTO %s (_id) VALUES (%d)" marker id))))

(defn- pg-ingest! [pg-url {:keys [table]} ^long doc-count ^long per-batch]
  (with-open [conn (pg-conn pg-url)
              ps (jdbc/prepare conn [(format "INSERT INTO %s (_id) VALUES (?)" table)])]
    (doseq [batch (partition-all per-batch (range doc-count))]
      (let [first-idx (long (first batch))]
        (when (zero? (rem first-idx 10000))
          (log/tracef "%s: %,d written" table first-idx)))

      (when (Thread/interrupted) (throw (InterruptedException.)))

      (jdbc/with-transaction [_ conn]
        (jdbc/execute-batch! ps (mapv vector batch))))

    (let [{actual :doc_count} (jdbc/execute-one! conn [(format "SELECT COUNT(*) doc_count FROM %s" table)])]
      (assert (= actual doc-count)
              (format "failed for %s: expected: %d, got: %d" table doc-count actual)))))

;; --- node ---

(defn- ->node-config [config-file ^Path node-dir node-topic]
  ;; the config file carries the `pg` remote and the `kafka` cluster; the logs are set here because
  ;; they have to be per-run, and storage because the restart the drain depends on needs it durable
  (doto (xtn/->config (or config-file
                          (throw (ex-info "pg-source-tx-overhead needs --config-file for the `pg` remote and the `kafka` cluster" {}))))
    (xtn/apply-config! :log [:kafka {:cluster kafka-cluster, :topic node-topic}])
    (xtn/apply-config! :storage [:local {:path (.resolve node-dir "objects")}])))

(defn- attach-source-db! [node ^Path node-dir {:keys [db slot publication topic]} indexer]
  (with-open [^Connection conn (jdbc/get-connection node)]
    ;; one `topic` gives both logs - the replica log defaults to `<topic>-replica`, and it is the
    ;; one every mirrored transaction goes through
    (execute! conn (format "ATTACH DATABASE %s WITH $$
storage: !Local
  path: %s
log: !Kafka
  cluster: %s
  topic: %s
externalSource: !Postgres
  remote: pg
  slotName: %s
  publicationName: %s%s
$$"
                           db
                           (.resolve node-dir "src-objects")
                           kafka-cluster topic
                           slot publication
                           (if (s/blank? indexer) "" (str "\n  indexer: " indexer))))))

(defn- marker-arrived? [node {:keys [db marker]} ^long id !last-error]
  (try
    ;; the table only exists once the source has mirrored its first row
    (boolean (seq (xt/q node [(format "SELECT _id FROM %s.public.%s WHERE _id = %d" db marker id)])))
    (catch Exception e
      (reset! !last-error (ex-message e))
      false)))

(defn- await-marker! [node names ^long id]
  (let [deadline-ms (+ (System/currentTimeMillis) (long drain-timeout-ms))
        ;; a database that failed to attach queries exactly like one that hasn't caught up yet, so
        ;; carry the last error into the timeout - otherwise a rejected ATTACH reads as a slow drain
        !last-error (atom nil)]
    (loop []
      (when (Thread/interrupted) (throw (InterruptedException.)))

      (cond
        (marker-arrived? node names id !last-error) nil

        (> (System/currentTimeMillis) deadline-ms)
        (throw (ex-info "timed out waiting for the marker to reach XT"
                        {:names names, :marker-id id, :last-error @!last-error}))

        :else (do (Thread/sleep (long drain-poll-ms)) (recur))))))

(defn- with-node [config-file ^Path node-dir node-topic f]
  (util/with-open [node (xtn/start-node (->node-config config-file node-dir node-topic))]
    (f node)))

;; --- reporting ---

(defn- log-drain [{:keys [table]} doc-count batch-size open-ms drain-ms]
  (let [doc-count (long doc-count)
        txs (quot doc-count (long batch-size))
        secs (/ (long drain-ms) 1000.0)]
    (log/infof "%s: drained %,d txs (%,d rows) in %.3fs - %,.0f tx/sec, %,.0f rows/sec (node open %,dms)"
               table txs doc-count secs (/ txs secs) (/ doc-count secs) open-ms)))

(defn- verify-rows! [node {:keys [db table]} ^long doc-count]
  (let [{:keys [n]} (first (xt/q node [(format "SELECT COUNT(*) n FROM %s.public.%s" db table)]))]
    (when-not (= doc-count n)
      (throw (ex-info "row count mismatch" {:table table, :expected doc-count, :actual n})))))

;; --- tasks ---

(defn- batch-size-tasks [{:keys [pg-url config-file indexer topic-prefix ^long doc-count]} ^long batch-size]
  (let [names (->names topic-prefix batch-size)
        !node-dir (delay (util/tmp-dir (str "xtdb-bench-pg-" batch-size)))]
    [{:t :call, :stage (keyword (str "setup-pg-batch-" batch-size)), :setup? true
      :f (fn [_] (setup-pg! pg-url names))}

     ;; attaching against the empty table leaves a snapshot-complete token behind, which is what
     ;; the drain resumes from. It is written between the last snapshot batch and the stream
     ;; opening, so it takes two markers to know it is durable rather than nearly so: marker 1
     ;; predates the slot and comes back through the snapshot, proving only that the slot exists;
     ;; marker 2 is written after that, so it is past the slot's consistent point, has to be
     ;; streamed, and its arrival puts the token behind us
     {:t :call, :stage (keyword (str "prime-batch-" batch-size)), :setup? true
      :f (fn [_]
           (write-marker! pg-url names snapshotted-id)
           (with-node config-file @!node-dir (:node-topic names)
             (fn [node]
               (attach-source-db! node @!node-dir names indexer)
               (await-marker! node names snapshotted-id)
               (write-marker! pg-url names streamed-id)
               (await-marker! node names streamed-id))))}

     {:t :call, :stage (keyword (str "pg-ingest-batch-" batch-size))
      :f (fn [_]
           (pg-ingest! pg-url names doc-count batch-size)
           (write-marker! pg-url names drained-id))}

     {:t :call, :stage (keyword (str "xt-drain-batch-" batch-size))
      :f (fn [_]
           (let [start-ms (System/currentTimeMillis)]
             (with-node config-file @!node-dir (:node-topic names)
               (fn [node]
                 (let [open-ms (- (System/currentTimeMillis) start-ms)]
                   (await-marker! node names drained-id)
                   (log-drain names doc-count batch-size open-ms
                              (- (System/currentTimeMillis) start-ms open-ms))
                   (verify-rows! node names doc-count))))))}

     {:t :call, :stage (keyword (str "cleanup-batch-" batch-size)), :setup? true
      :f (fn [_] (util/delete-dir @!node-dir))}]))

(defmethod b/cli-flags :pg-source-tx-overhead [_]
  [["-dc" "--doc-count DOCUMENT_COUNT" "Number of documents to ingest"
    :parse-fn parse-long
    :default 100000]

   ["-bs" "--batch-sizes BATCH_SIZES" "Batch sizes to use for ingestion, e.g. \"1000,100,10,1\""
    :parse-fn #(->> (s/split % #",") (map parse-long) (into #{}))
    :default #{1000 100 10 1}]

   [nil "--pg-url JDBC_URL" "Postgres to write into - has to be the server the node config's `pg` remote names"
    :default default-pg-url]

   [nil "--source-indexer INDEXER" "`indexer:` for the attached source - empty to omit it, for nodes predating the key"
    :default default-indexer]

   ["-h" "--help"]])

(defn benchmark [{:keys [seed doc-count batch-sizes pg-url source-indexer config-file]
                  :or {seed 0, doc-count 100000, batch-sizes #{1000 100 10 1}
                       pg-url default-pg-url, source-indexer default-indexer}}]
  (log/info {:doc-count doc-count, :batch-sizes batch-sizes})

  (let [topic-prefix (->topic-prefix)
        opts {:pg-url pg-url, :config-file config-file, :topic-prefix topic-prefix
              :indexer source-indexer, :doc-count doc-count}]
    {:title "Postgres source tx overhead"
     :benchmark-type :pg-source-tx-overhead
     :seed seed
     ;; the indexer goes in the parameters because it is what differs between a run against an
     ;; older node and one against this tree, and the two runs get compared; the topic prefix so
     ;; that a run's Kafka topics can be found afterwards
     :parameters {:doc-count doc-count, :batch-sizes (sort > batch-sizes)
                  :source-indexer source-indexer, :topic-prefix topic-prefix}
     :tasks (into [] (mapcat #(batch-size-tasks opts %)) (sort > batch-sizes))}))

(defmethod b/->benchmark :pg-source-tx-overhead [_ opts]
  (benchmark opts))

(comment
  ;; needs `docker-compose up postgres`
  (require '[clojure.java.io :as io])

  ;; every stage opens and closes its own node, so there is nothing to hand a running one to
  ((b/compile-benchmark (benchmark {:doc-count 10000, :batch-sizes #{1000 1}
                                    :config-file (io/file "modules/bench/config/pg-source.yaml")}))
   nil))
