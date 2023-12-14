(ns xtdb.stagnant-log-flusher
  (:require [clojure.tools.logging :as log]
            [xtdb.log :as xt-log]
            [xtdb.indexer]
            [xtdb.node :as xtn]
            [xtdb.tx-producer]
            [juxt.clojars-mirrors.integrant.core :as ig]
            [xtdb.util :as util])
  (:import (java.nio ByteBuffer)
           (java.nio.channels ClosedByInterruptException)
           (java.time Duration)
           (java.util.concurrent ExecutorService Executors ThreadFactory TimeUnit)
           (xtdb.indexer IIndexer)
           (xtdb.log Log)))

;; see https://github.com/xtdb/xtdb/issues/2548

(defmethod ig/prep-key ::flusher
  [_ opts]
  (into {:indexer (ig/ref :xtdb/indexer)
         :log (ig/ref :xtdb/log)
         :duration #time/duration "PT4H"
         :on-heartbeat (constantly nil)}
        opts))

(defmethod ig/init-key ::flusher
  [_ {:keys [^IIndexer indexer
             ^Log log
             duration
             ;; callback hook used to control timing in tests
             ;; receives a map of the :last-flush, :last-seen tx-keys
             on-heartbeat]}]
  (let [exr-tf (util/->prefix-thread-factory "xtdb.stagnant-log-flush")
        exr (Executors/newSingleThreadScheduledExecutor exr-tf)

        ;; the tx-key of the last seen chunk tx
        previously-seen-chunk-tx-id (atom nil)
        ;; the tx-key of the last flush msg sent by me
        !last-flush-tx-id (atom nil)

        ^Runnable f
        (bound-fn heartbeat []
          (on-heartbeat {:last-flush @!last-flush-tx-id, :last-seen @previously-seen-chunk-tx-id})
          (when-some [latest-tx-id (some-> (.latestCompletedTx indexer) (.txId))]
            (let [latest-chunk-tx-id (some-> (.latestCompletedChunkTx indexer) (.txId))]
              (try
                (when (and (= @previously-seen-chunk-tx-id latest-chunk-tx-id)
                           (or (nil? @!last-flush-tx-id)
                               (< @!last-flush-tx-id latest-tx-id)))
                  (log/infof "last chunk tx-id %s, flushing any pending writes" latest-chunk-tx-id)

                  (let [record-buf (-> (ByteBuffer/allocate 9)
                                       (.put (byte xt-log/hb-flush-chunk))
                                       (.putLong (or latest-chunk-tx-id -1))
                                       .flip)
                        record @(.appendRecord log record-buf)]
                   (reset! !last-flush-tx-id (some-> (:tx record) (.txId)))))
                (catch InterruptedException _)
                (catch ClosedByInterruptException _)
                (catch Throwable e
                  (log/error e "exception caught submitting flush record"))
                (finally
                  (reset! previously-seen-chunk-tx-id latest-chunk-tx-id))))))]
    {:executor exr
     :task (.scheduleAtFixedRate exr f (.toMillis ^Duration duration) (.toMillis ^Duration duration) TimeUnit/MILLISECONDS)}))

(defmethod ig/halt-key! ::flusher [_ {:keys [^ExecutorService executor, task]}]
  (future-cancel task)
  (.shutdown executor)
  (let [timeout-secs 10]
    (when-not (.awaitTermination executor timeout-secs TimeUnit/SECONDS)
      (log/warnf "flusher did not shutdown within %d seconds" timeout-secs))))

(comment

  ((requiring-resolve 'xtdb.test-util/set-log-level!)
   'xtdb.indexer :debug)

  (require 'xtdb.node)

  (def sys
    (-> (node/node-system {::flusher {:duration #time/duration "PT5S"}})
        (ig/prep)
        (ig/init [::flusher])))

  (ig/halt! sys)

  (::flusher sys)

  (.latestCompletedTx (:xtdb/indexer sys))

  (defn submit [tx] (.submitTx (:xtdb.tx-producer/tx-producer sys) tx {}))

  @(submit [(xt/put :foo {:xt/id 42, :msg "Hello, world!"})])

  )
