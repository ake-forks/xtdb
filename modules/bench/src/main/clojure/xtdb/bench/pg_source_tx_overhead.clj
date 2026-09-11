(ns xtdb.bench.pg-source-tx-overhead
  "The IngestTsOverhead sweep run through CDC: writes `doc-count` rows into Postgres at each
   batch size and measures how long they take to reach the XTDB database mirroring it.

   `pg-ingest-batch-N` is the write into Postgres and `xt-mirror-batch-N` is what was still
   outstanding when that write stopped; per-transaction latency comes from the source's own
   `commit_lag_seconds` summary, logged at the end of each mirror stage.

   See modules/bench/README.adoc for what a run needs and what it takes over in Postgres.

   Run: ./gradlew pg-source-tx-overhead -PdocCount=100000 [-PbatchSizes=1000,1] [-Pyourkit]"
  (:require [clojure.string :as s]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [xtdb.api :as xt]
            [xtdb.bench :as b])
  (:import [io.micrometer.core.instrument DistributionSummary Meter MeterRegistry]
           [java.sql Connection Statement]))

(def ^:private db-name "bench_pg_src")
(def ^:private slot-name "xtdb_bench_pg_source")
(def ^:private publication-name "xtdb_bench_pg_source")
(def ^:private sentinel-table "sentinel")

(def ^:private default-pg-url "jdbc:postgresql://localhost:5433/postgres?user=postgres&password=postgres")

;; only ever waited out after the writer has stopped, so this catches a stalled source
;; rather than budgeting for a slow one
(def ^:private mirror-timeout-ms (* 10 60 1000))

(defn- table-name [^long batch-size] (str "batched_" batch-size))

(defn- pg-conn ^Connection [pg-url]
  (jdbc/get-connection {:jdbcUrl pg-url}))

(defn- execute! [^Connection conn & sqls]
  (with-open [^Statement stmt (.createStatement conn)]
    (doseq [^String sql sqls]
      (.execute stmt sql))))

(defn- setup-pg! [pg-url batch-sizes]
  (let [tables (conj (mapv table-name (sort > batch-sizes)) sentinel-table)]
    (with-open [conn (pg-conn pg-url)]
      (apply execute! conn
             (concat [(format "DROP PUBLICATION IF EXISTS %s" publication-name)

                      ;; a slot outlives the node that created it, so the previous run's is still
                      ;; holding WAL - reclaim it here, where nothing has it open
                      (format "SELECT pg_drop_replication_slot('%s') FROM pg_replication_slots WHERE slot_name = '%s'"
                              slot-name slot-name)]

                     (mapcat (fn [table]
                               [(format "DROP TABLE IF EXISTS %s" table)
                                (format "CREATE TABLE %s (_id BIGINT PRIMARY KEY)" table)])
                             tables)

                     [(format "CREATE PUBLICATION %s FOR TABLE %s" publication-name (s/join ", " tables))])))))

(defn- attach-source-db! [node]
  (with-open [^Connection conn (jdbc/get-connection node)]
    (execute! conn (format "ATTACH DATABASE %s WITH $$
externalSource: !Postgres
  remote: pg
  slotName: %s
  publicationName: %s
  indexer: !DirectMirror {}
$$"
                           db-name slot-name publication-name))))

(defn- xt-row-count [node table]
  (or (try
        ;; the table only exists once the source has mirrored its first row
        (:n (first (xt/q node [(format "SELECT COUNT(*) n FROM %s.public.%s" db-name table)])))
        (catch Exception _ nil))
      0))

(defn- await-rows! [node table ^long expected]
  (let [start-ms (System/currentTimeMillis)
        deadline-ms (+ start-ms (long mirror-timeout-ms))]
    (loop []
      (when (Thread/interrupted) (throw (InterruptedException.)))

      (let [n (long (xt-row-count node table))
            now-ms (System/currentTimeMillis)]
        (cond
          (>= n expected) (log/infof "%s: %,d rows mirrored, %.3fs behind the last write"
                                     table n (/ (- now-ms start-ms) 1000.0))

          (> now-ms deadline-ms) (throw (ex-info "timed out waiting for rows to reach XT"
                                                 {:table table, :expected expected, :actual n}))

          :else (do (Thread/sleep 200) (recur)))))))

(defn- await-streaming! [node pg-url]
  ;; rows committed before the slot goes live arrive in the source's initial snapshot instead of
  ;; its stream, and the snapshot neither records a commit lag nor is what this measures
  (with-open [conn (pg-conn pg-url)]
    (execute! conn (format "INSERT INTO %s (_id) VALUES (0)" sentinel-table)))
  (await-rows! node sentinel-table 1))

(defn- pg-ingest! [conn table ^long doc-count ^long per-batch]
  (with-open [ps (jdbc/prepare conn [(format "INSERT INTO %s (_id) VALUES (?)" table)])]
    (doseq [batch (partition-all per-batch (range doc-count))]
      (let [first-idx (long (first batch))]
        (when (zero? (rem first-idx 1000))
          (log/trace :done first-idx)))

      (when (Thread/interrupted) (throw (InterruptedException.)))

      (jdbc/with-transaction [_ conn]
        (jdbc/execute-batch! ps (mapv vector batch)))))

  (let [{actual :doc_count} (jdbc/execute-one! conn [(format "SELECT COUNT(*) doc_count FROM %s" table)])]
    (assert (= actual doc-count)
            (format "failed for %s: expected: %d, got: %d" table doc-count actual))))

(defn- commit-lag-totals []
  (when-let [^MeterRegistry reg b/*registry*]
    (when-let [^DistributionSummary summary (->> (.getMeters reg)
                                                 (filter #(= "xtdb.postgres_source.commit_lag_seconds"
                                                             (.getName (.getId ^Meter %))))
                                                 first)]
      {:count (.count summary), :total-seconds (.totalAmount summary)})))

(defn- log-commit-lag [before after]
  (when (and before after)
    (let [txs (- (long (:count after)) (long (:count before)))
          total-seconds (- (double (:total-seconds after)) (double (:total-seconds before)))]
      (when (pos? txs)
        (log/infof "commit lag: %,d txs, mean %.1fms" txs (/ (* 1000.0 total-seconds) txs))))))

(defn- batch-size-tasks [pg-url ^long doc-count ^long batch-size]
  (let [table (table-name batch-size)
        !lag-before (atom nil)]
    [{:t :call
      :stage (keyword (str "pg-ingest-batch-" batch-size))
      :f (fn [_]
           (reset! !lag-before (commit-lag-totals))
           (with-open [conn (pg-conn pg-url)]
             (pg-ingest! conn table doc-count batch-size)))}

     {:t :call
      :stage (keyword (str "xt-mirror-batch-" batch-size))
      :f (fn [{:keys [node]}]
           (await-rows! node table doc-count)
           (log-commit-lag @!lag-before (commit-lag-totals)))}]))

(defmethod b/cli-flags :pg-source-tx-overhead [_]
  [["-dc" "--doc-count DOCUMENT_COUNT" "Number of documents to ingest"
    :parse-fn parse-long
    :default 100000]

   ["-bs" "--batch-sizes BATCH_SIZES" "Batch sizes to use for ingestion, e.g. \"1000,100,10,1\""
    :parse-fn #(->> (s/split % #",") (map parse-long) (into #{}))
    :default #{1000 100 10 1}]

   [nil "--pg-url JDBC_URL" "Postgres to write into - has to be the server the node config's `pg` remote names"
    :default default-pg-url]

   ["-h" "--help"]])

(defn benchmark [{:keys [seed doc-count batch-sizes pg-url]
                  :or {seed 0, doc-count 100000, batch-sizes #{1000 100 10 1}, pg-url default-pg-url}}]
  (log/info {:doc-count doc-count, :batch-sizes batch-sizes})

  {:title "Postgres source tx overhead"
   :benchmark-type :pg-source-tx-overhead
   :seed seed
   :parameters {:doc-count doc-count, :batch-sizes (sort > batch-sizes)}
   :tasks (into [{:t :call, :stage :setup-pg, :setup? true
                  :f (fn [_] (setup-pg! pg-url batch-sizes))}

                 {:t :call, :stage :attach, :setup? true
                  :f (fn [{:keys [node]}] (attach-source-db! node))}

                 {:t :call, :stage :await-streaming, :setup? true
                  :f (fn [{:keys [node]}] (await-streaming! node pg-url))}]

                (mapcat #(batch-size-tasks pg-url doc-count %) (sort > batch-sizes)))})

(defmethod b/->benchmark :pg-source-tx-overhead [_ opts]
  (benchmark opts))

(comment
  ;; needs `docker-compose up postgres`
  (require '[clojure.java.io :as io]
           '[xtdb.node :as xtn]
           '[xtdb.util :as util])

  ;; a run owns its node start to finish: the slot setup-pg! reclaims is still held by the
  ;; source of any node that is up, and the database it attaches is already attached there
  (with-open [node (xtn/start-node (io/file "modules/bench/config/pg-source.yaml"))]
    (binding [b/*registry* (.getMeterRegistry (util/node-base node))]
      ((b/compile-benchmark (benchmark {:doc-count 10000, :batch-sizes #{1000 1}}))
       node))))
