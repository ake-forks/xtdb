#!/usr/bin/env bb

;; Tabulates a pg-source-tx-overhead run, or compares two of them.
;;
;;   ./pg-source-table.clj run.jsonl
;;   ./pg-source-table.clj head.jsonl b37e120.jsonl
;;
;; Takes either a --bench-log-file (JSON per line) or a whole console log - the drain times are
;; only logged in full, so a console log gives the sharper number and a bench log falls back to
;; the stage time, which also covers node startup, the row-count check and the node close.

(require '[cheshire.core :as json]
         '[clojure.string :as str])

(def ^:private drained-re
  #"batched_(\d+): drained ([\d,]+) txs \(([\d,]+) rows\) in ([\d.]+)s")

(def ^:private stage-re #"^(pg-ingest|xt-drain)-batch-(\d+)$")

(defn- ->long [s] (parse-long (str/replace s "," "")))

(defn- read-run [path]
  (reduce (fn [run line]
            (cond
              (str/starts-with? line "{")
              (let [{:strs [stage time-taken-ms parameters]} (json/parse-string line)]
                (cond-> run
                  parameters (assoc :parameters parameters)

                  (and stage time-taken-ms)
                  (as-> run (if-let [[_ phase batch] (re-matches stage-re stage)]
                              (assoc-in run [:stages (->long batch) (keyword phase)] time-taken-ms)
                              run))))

              ;; the benchmark's own line, which excludes everything but the drain itself
              :else
              (if-let [[_ batch txs _rows secs] (re-find drained-re line)]
                (-> run
                    (assoc-in [:stages (->long batch) :drain-ms] (long (* 1000 (parse-double secs))))
                    (assoc-in [:stages (->long batch) :txs] (->long txs)))
                run)))
          {:label (-> path (str/split #"/") last), :stages {}}
          (str/split-lines (slurp path))))

(defn- rows [{:keys [parameters stages]}]
  (let [doc-count (get parameters "doc-count")]
    (for [[batch {:keys [pg-ingest xt-drain drain-ms txs]}] (sort-by (comp - key) stages)
          :let [txs (or txs (quot (long doc-count) (long batch)))
                drain (or drain-ms xt-drain)]]
      {:batch batch, :txs txs, :docs doc-count
       :pg-ms pg-ingest, :drain-ms drain
       :precise? (some? drain-ms)
       :pg-tx-s (when (and pg-ingest (pos? (long pg-ingest))) (/ txs (/ (long pg-ingest) 1000.0)))
       :drain-tx-s (when (and drain (pos? (long drain))) (/ txs (/ (long drain) 1000.0)))
       :drain-rows-s (when (and drain (pos? (long drain))) (/ (long doc-count) (/ (long drain) 1000.0)))})))

;; Per-transaction cost against rows-per-transaction, pair by pair. A single regression would put a
;; confident number on any two points, including a batch size whose drain was too short to amortise
;; the node and consumer startup - so the pairs are printed, and only their agreement licenses the
;; per-row figure. Agreement is the evidence that `fixed + per-row × rows` describes the run at all.
(defn- per-tx-µs [{:keys [drain-ms txs]}]
  (when (and drain-ms txs (pos? (long txs)))
    (/ (* 1000.0 (long drain-ms)) (long txs))))

;; Every pair is anchored on the largest batch size, because that is the only point where row work
;; is a real fraction of a transaction's cost - at batch 1000 it is about half, at batch 10 under a
;; percent. Differencing two small batch sizes measures the noise on the fixed cost and nothing else.
(defn- pairwise-slopes [rs]
  (let [pts (->> rs (keep #(when-let [c (per-tx-µs %)] [(long (:batch %)) c])) (sort-by first))
        [b-max c-max] (last pts)]
    (for [[b c] (butlast pts)]
      {:from b, :to b-max, :per-row (/ (- c-max c) (- b-max b)), :base-cost c})))

(defn- cost-report [label rs]
  (let [slopes (pairwise-slopes rs)]
    (when (seq slopes)
      (println label)
      (doseq [{:keys [from to per-row]} slopes]
        (println (format "  batch %,d -> %,d: %.1fµs per row" from to per-row)))

      (let [per-rows (map :per-row slopes)
            spread (/ (apply max per-rows) (max 1e-9 (apply min per-rows)))]
        (cond
          (< (count slopes) 2)
          (println "  (need three batch sizes before the per-row cost can be checked against itself)")

          (<= spread 1.5)
          (let [per-row (/ (reduce + per-rows) (count per-rows))
                {:keys [from base-cost]} (first slopes)]
            (println (format "  consistent: ~%.1fµs per row, ~%,.0fµs fixed per transaction"
                             per-row (- base-cost (* per-row from)))))

          :else
          (println (format "  inconsistent (%.1fx spread) - a batch size is dominated by something other than row work"
                           spread)))))))

(defn- fmt [width s] (format (str "%" width "s") (str s)))

(defn- print-table [headers widths rs]
  (println (str/join "  " (map fmt widths headers)))
  (println (str/join "  " (map #(apply str (repeat % \-)) widths)))
  (doseq [r rs] (println (str/join "  " (map fmt widths r)))))

(defn- single [run]
  (let [rs (rows run)]
    (println (format "\n%s - %,d docs, indexer %s\n"
                     (:label run)
                     (long (get-in run [:parameters "doc-count"]))
                     (pr-str (get-in run [:parameters "source-indexer"] ""))))
    (print-table ["batch" "txs" "pg ingest" "pg tx/s" "µs/tx" "xt drain" "drain tx/s" "rows/s" "µs/tx"]
                 [6 8 10 9 8 9 11 10 8]
                 (for [{:keys [batch txs pg-ms drain-ms pg-tx-s drain-tx-s drain-rows-s precise?]} rs]
                   [batch (format "%,d" txs)
                    (format "%,dms" (long pg-ms))
                    (if pg-tx-s (format "%,.0f" pg-tx-s) "-")
                    (if pg-tx-s (format "%,.0f" (/ 1e6 pg-tx-s)) "-")
                    (str (format "%,dms" (long drain-ms)) (if precise? "" "*"))
                    (if drain-tx-s (format "%,.0f" drain-tx-s) "-")
                    (if drain-rows-s (format "%,.0f" drain-rows-s) "-")
                    (if drain-tx-s (format "%,.0f" (/ 1e6 drain-tx-s)) "-")]))
    (println)
    (cost-report "drain cost, batch size against batch size:" rs)

    (when (some (complement :precise?) rs)
      (println "\n* stage time - includes node startup, the row-count check and the node close."
               "\n  Run against a console log for the drain alone."))))

(defn- compare-runs [a b]
  (let [ra (into {} (map (juxt :batch identity)) (rows a))
        rb (into {} (map (juxt :batch identity)) (rows b))]
    (println (format "\nA = %s   B = %s\n" (:label a) (:label b)))
    (print-table ["batch" "txs" "A pg" "B pg" "A drain" "B drain" "A tx/s" "B tx/s" "B/A"]
                 [6 8 9 9 9 9 9 9 6]
                 (for [batch (sort > (distinct (concat (keys ra) (keys rb))))
                       :let [{a-pg :pg-ms, a-dr :drain-ms, a-tx :drain-tx-s} (ra batch)
                             {b-pg :pg-ms, b-dr :drain-ms, b-tx :drain-tx-s} (rb batch)]]
                   [batch
                    (format "%,d" (or (:txs (ra batch)) (:txs (rb batch))))
                    (if a-pg (format "%,dms" (long a-pg)) "-")
                    (if b-pg (format "%,dms" (long b-pg)) "-")
                    (if a-dr (format "%,dms" (long a-dr)) "-")
                    (if b-dr (format "%,dms" (long b-dr)) "-")
                    (if a-tx (format "%,.0f" a-tx) "-")
                    (if b-tx (format "%,.0f" b-tx) "-")
                    (if (and a-tx b-tx) (format "%.2fx" (/ b-tx a-tx)) "-")]))

    (println)
    (cost-report "A drain cost:" (rows a))
    (println)
    (cost-report "B drain cost:" (rows b))))

(let [[a b & more] *command-line-args*]
  (when (or (nil? a) (seq more))
    (println "usage: pg-source-table.clj <run.jsonl|console.log> [<run2>]")
    (System/exit 2))

  (doseq [f (remove nil? [a b])
          :when (not (.exists (java.io.File. ^String f)))]
    (println "no such file:" f)
    (System/exit 2))

  (if b
    (compare-runs (read-run a) (read-run b))
    (single (read-run a)))
  (println))
