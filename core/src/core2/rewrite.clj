(ns core2.rewrite
  (:require [clojure.walk :as w]
            [clojure.core.match :as m])
  (:import java.util.regex.Pattern
           [java.util ArrayList HashMap List Map]
           [clojure.lang Box IPersistentVector ILookup MapEntry]
           core2.BitUtil))

(set! *unchecked-math* :warn-on-boxed)

(deftype Zip [node ^int idx parent ^:unsynchronized-mutable ^int hash_]
  ILookup
  (valAt [_ k]
    (when (= :node k)
      node))

  (valAt [_ k not-found]
    (if (= :node k)
      node
      not-found))

  Object
  (equals [this other]
    (if (identical? this other)
      true
      (let [^Zip other other]
        (and (instance? Zip other)
             (= (.node this) (.node other))
             (= (.idx this) (.idx other))
             (= (.parent this) (.parent other))))))

  (hashCode [this]
    (if (zero? hash_)
      (let [result 1
            result (+ (* 31 result) (if node
                                      (long (.hashCode node))
                                      0))
            result (+ (* 31 result) (long (Integer/hashCode idx)))
            result (+ (* 31 result) (if parent
                                      (long (.hashCode parent))
                                      0))
            result (unchecked-int result)]
        (set! (.hash_ this) result)
        result)
      hash_)))

(defn ->zipper [x]
  (if (instance? Zip x)
    x
    (Zip. x -1 nil 0)))

(defn- zupdate-parent [^Zip z]
  (when-let [^Zip parent (.parent z)]
    (let [node (.node z)
          ^List level (.node parent)
          idx (.idx z)]
      (if (identical? node (.get level idx))
        parent
        (Zip. (.assocN ^IPersistentVector level idx node) (.idx parent) (.parent parent) 0)))))

(defn znode [^Zip z]
  (.node z))

(defn zbranch? [^Zip z]
  (vector? (.node z)))

(defn zleft [^Zip z]
  (when-let [^Zip parent (zupdate-parent z)]
    (let [idx (dec (.idx z))]
      (when (BitUtil/bitNot (neg? idx))
        (let [^List level (.node parent)]
          (Zip. (.get level idx) idx parent 0))))))

(defn zright [^Zip z]
  (when-let [^Zip parent (zupdate-parent z)]
    (let [idx (inc (.idx z))
          ^List level (.node parent)]
      (when (< idx (.size level))
        (Zip. (.get level idx) idx parent 0)))))

(defn- zright-no-edit [^Zip z]
  (when-let [^Zip parent (.parent z)]
    (let [idx (inc (.idx z))
          ^List level (.node parent)]
      (when (< idx (.size level))
        (Zip. (.get level idx) idx parent 0)))))

(defn znth [^Zip z ^long idx]
  (when (zbranch? z)
    (let [^List node (.node z)
          idx (if (neg? idx)
                (+ (.size node) idx)
                idx)]
      (when (and (< idx (.size node))
                 (BitUtil/bitNot (neg? idx)))
        (Zip. (.get node idx) idx z 0)))))

(defn zdown [^Zip z]
  (let [node (.node z)]
    (when (and (zbranch? z)
               (BitUtil/bitNot (.isEmpty ^List node)))
      (Zip. (.get ^List node 0) 0 z 0))))

(defn zup [^Zip z]
  (let [idx (.idx z)]
    (when (BitUtil/bitNot (neg? idx))
      (zupdate-parent z))))

(defn- zdepth ^long [^Zip z]
  (loop [z z
         n 0]
    (if-let [p (.parent z)]
      (recur p (inc n))
      n)))

(defn- zups [^Zip z ^long n]
  (if (zero? n)
    z
    (recur (zup z) (dec n))))

(defn zroot [^Zip z]
  (if-let [z (zup z)]
    (recur z)
    (.node z)))

(defn- zright-or-up [^Zip z ^long depth out-fn]
  (when z
    (if (pos? depth)
      (when-let [z (out-fn z)]
        (if (reduced? z)
          @z
          (or (zright z)
              (recur (zup z) (dec depth) out-fn))))
      (reduced z))))

(defn znext
  ([z ^long depth]
   (znext z depth identity))
  ([z ^long depth out-fn]
   (or (zdown z)
       (zright-or-up z depth out-fn))))

(defn- zright-or-up-no-edit [^Zip z top]
  (loop [z z]
    (when z
      (when-not (identical? z top)
        (or (zright-no-edit z)
            (recur (.parent z)))))))

(defn- znext-no-edit [z top]
  (or (zdown z)
      (zright-or-up-no-edit z top)))

(defn zprev [z]
  (if-let [z (zleft z)]
    (loop [z z]
      (if-let [z (znth z -1)]
        (recur z)
        z))
    (zup z)))

(defn zchild-idx ^long [^Zip z]
  (when z
    (.idx z)))

(defn zchildren [^Zip z]
  (vec (.node ^Zip (.parent z))))

(defn zrights [^Zip z]
  (let [idx (inc (.idx z))
        ^List children (zchildren z)]
    (when (< idx (.size children))
      (seq (subvec children idx)))))

(defn zlefts [^Zip z]
  (seq (subvec (zchildren z) 0 (.idx z))))

(defn zreplace [^Zip z x]
  (when z
    (if (identical? (.node z) x)
      z
      (Zip. x (.idx z) (.parent z) 0))))

;; Zipper pattern matching

(derive ::m/zip ::m/vector)

(defmethod m/nth-inline ::m/zip
  [t ocr i]
  `(let [^Zip z# ~ocr]
     (Zip. (.get ^List (.node z#) ~i) ~i z# 0)))

(defmethod m/count-inline ::m/zip
  [t ocr]
  `(let [^Zip z# ~ocr]
     (if (zbranch? z#)
       (.size ^List (znode z#))
       0)))

(defmethod m/subvec-inline ::m/zip
  ([_ ocr start] (throw (UnsupportedOperationException.)))
  ([_ ocr start end] (throw (UnsupportedOperationException.))))

(defmethod m/tag ::m/zip
  [_] "core2.rewrite.Zip")

(defmacro zmatch {:style/indent 1} [z & clauses]
  (let [pattern+exprs (partition 2 clauses)
        else-clause (when (odd? (count clauses))
                      (last clauses))
        variables (->> pattern+exprs
                       (map first)
                       (flatten)
                       (filter symbol?))
        zip-matches? (some (comp :z meta) variables)
        shadowed-locals (filter (set variables) (keys &env))]
    (when-not (empty? shadowed-locals)
      (throw (IllegalArgumentException. (str "Match variables shadow locals: " (set shadowed-locals)))))
    (if-not zip-matches?
      `(let [z# ~z
             node# (if (instance? Zip z#)
                     (znode z#)
                     z#)]
         (m/match node#
                  ~@(->> (for [[pattern expr] pattern+exprs]
                           [pattern expr])
                         (reduce into []))
                  :else ~else-clause))
      `(m/matchv ::m/zip [(->zipper ~z)]
                 ~@(->> (for [[pattern expr] pattern+exprs]
                          [[(w/postwalk
                             (fn [x]
                               (cond
                                 (symbol? x)
                                 (if (or (= '_ x)
                                         (:z (meta x)))
                                   x
                                   {:node x})

                                 (vector? x)
                                 x

                                 :else
                                 {:node x}))
                             pattern)]
                           expr])
                        (reduce into []))
                 :else ~else-clause))))

;; Attribute Grammar spike.

;; See related literature:
;; https://inkytonik.github.io/kiama/Attribution (no code is borrowed)
;; https://arxiv.org/pdf/2110.07902.pdf
;; https://haslab.uminho.pt/prmartins/files/phd.pdf
;; https://github.com/christoff-buerger/racr

(defn ctor [ag]
  (when ag
    (let [node (znode ag)]
      (when (vector? node)
        (.nth ^IPersistentVector node 0 nil)))))

(defn ctor? [kw ag]
  (= kw (ctor ag)))

(defmacro vector-zip [x]
  `(->zipper ~x))
(defmacro node [x]
  `(znode ~x))
(defmacro root [x]
  `(zroot ~x))
(defmacro left [x]
  `(zleft ~x))
(defmacro right [x]
  `(zright ~x))
(defmacro prev [x]
  `(zprev ~x))
(defmacro parent [x]
  `(zup ~x))
(defmacro $ [x n]
  `(znth ~x ~n))
(defmacro child-idx [x]
  `(zchild-idx ~x))

(defn lexeme [ag ^long n]
  (some-> ($ ag n) (znode)))

(defn first-child? [ag]
  (= 1 (count (zlefts ag))))

(defn left-or-parent [ag]
  (if (first-child? ag)
    (parent ag)
    (zleft ag)))

(defmacro zcase {:style/indent 1} [ag & body]
  `(case (ctor ~ag) ~@body))

(def ^:dynamic *memo*)

(defn zmemoize-with-inherited [f]
  (fn [x]
    (let [^Map memo *memo*
          memo-box (Box. nil)]
      (loop [x x
             inherited? false]
        (let [k (MapEntry/create f x)
              ^Box stored-memo-box (.getOrDefault memo k memo-box)]
          (if (identical? memo-box stored-memo-box)
            (let [v (f x)]
              (.put memo k memo-box)
              (if (= ::inherit v)
                (some-> x (parent) (recur true))
                (do (set! (.val memo-box) v)
                    v)))
            (let [v (.val stored-memo-box)]
              (when inherited?
                (set! (.val memo-box) v))
              v)))))))

(defn zmemoize [f]
  (fn [x]
    (let [^Map memo *memo*
          k (MapEntry/create f x)
          v (.getOrDefault memo k ::not-found)]
      (if (= ::not-found v)
        (let [v (f x)]
          (if (= ::inherit v)
            (some-> x (parent) (recur))
            (doto v
              (->> (.put memo k)))))
        v))))

;; Strategic Zippers based on Ztrategic

;; https://arxiv.org/pdf/2110.07902.pdf
;; https://www.di.uminho.pt/~joost/publications/SBLP2004LectureNotes.pdf

;; Strafunski:
;; https://www.di.uminho.pt/~joost/publications/AStrafunskiApplicationLetter.pdf
;; https://arxiv.org/pdf/cs/0212048.pdf
;; https://arxiv.org/pdf/cs/0204015.pdf
;; https://arxiv.org/pdf/cs/0205018.pdf

;; Type Preserving

(defn seq-tp
  ([x y]
   (fn [z]
     (seq-tp x y z)))
  ([x y z]
   (when-let [z (x z)]
     (y z))))

(defn choice-tp
  ([x y]
   (fn [z]
     (choice-tp x y z)))
  ([x y z]
   (if-let [z (x z)]
     z
     (y z))))

;; (defn ->cont-fn [f]
;;   (fn [x k]
;;     (fn []
;;       (k (f x)))))

;; (defn ->cont-seq-tp [a b]
;;   (fn [x k]
;;     (fn []
;;       (a x (fn [x]
;;              (when x
;;                (fn []
;;                  (b x k))))))))

;; (defn ->cont-choice-tp [a b]
;;   (fn [x k]
;;     (fn []
;;       (a x (fn [x2]
;;              (if x2
;;                (fn []
;;                  (k x2))
;;                (fn []
;;                  (b x k))))))))

;; (trampoline (->cont-choice-tp
;;              (->cont-fn inc)
;;              (->cont-fn /))
;;             2
;;             identity)

;; (defn ->cont-all-tp [f]
;;   (->cont-choice-tp
;;    (->cont-seq-tp
;;     (->cont-fn zdown)
;;     (->cont-choice-tp
;;      (letfn [(lp [x k]
;;                ((->cont-seq-tp f
;;                                (->cont-choice-tp
;;                                 (->cont-seq-tp (->cont-fn zright) lp)
;;                                 (->cont-fn zup)))
;;                 x
;;                 k))]
;;        lp)
;;      (fn [x k])))
;;    (fn [x k]
;;      x)))

;; (defn ->cont-full-tp-td [f]
;;   (letfn [(self [x k]
;;             ((->cont-seq-tp f (->cont-all-tp self)) x k))]
;;     self))

;; (trampoline (->cont-full-tp-td (->cont-fn (fn [x]
;;                                             (prn x)
;;                                             x)))
;;             (vector-zip [[1 :+ 2] :* [3 :- 4]])
;;             identity)

(defn all-tp
  ([f]
   (fn [z]
     (all-tp f z)))
  ([f z]
   (if-let [d (zdown z)]
     (loop [z d]
       (when-let [z (f z)]
         (if-let [r (zright z)]
           (recur r)
           (zup z))))
     z)))

(defn full-td-tp
  ([f]
   (fn self [z]
     (let [depth (zdepth z)]
       (loop [z z
              n 0]
         (when-let [z (f z)]
           (let [z (znext z n)]
             (if (reduced? z)
               @z
               (recur z (- (zdepth z) depth)))))))))
  ([f z]
   ((full-td-tp f) z)))

(defn full-bu-tp
  ([f]
   (fn self [z]
     (let [depth (zdepth z)]
       (loop [z z
              n 0]
         (when-let [z (znext z n f)]
           (if (reduced? z)
             (f @z)
             (recur z (- (zdepth z) depth))))))))
  ([f z]
   ((full-bu-tp f) z)))

(defn once-td-tp
  ([f]
   (fn self [z]
     (let [depth (zdepth z)]
       (loop [z z
              n 0]
         (if-let [z (f z)]
           (zups z n)
           (when-let [z (znext z n)]
             (when-not (reduced? z)
               (recur z (- (zdepth z) depth)))))))))
  ([f z]
   ((once-td-tp f) z)))

(defn z-try-apply-m [f]
  (fn [z]
    (some->> (f z)
             (zreplace z))))

(defn adhoc-tp [f g]
  (choice-tp (z-try-apply-m g) f))

(defn id-tp [x] x)

(defn fail-tp [_])

(def mono-tp (partial adhoc-tp fail-tp))

(defn try-tp [f]
  (choice-tp f id-tp))

(defn repeat-tp
  ([f]
   (fn [z]
     (if-let [z (f z)]
       (recur z)
       z)))
  ([f z]
   ((repeat-tp f) z)))

(defn innermost
  ([f]
   (fn self [z]
     (when z
       (let [depth (zdepth z)
             f2 (fn [z]
                  (if-let [z (f z)]
                    (reduced z)
                    z))]
         (loop [z z
                n 0]
           (if-let [z (znext z n f2)]
             (if (reduced? z)
               (if-let [z (f @z)]
                 (recur z n)
                 @z)
               (recur z (- (zdepth z) depth)))))))))
  ([f z]
   ((innermost f) z)))

(defn outermost
  ([f]
   (repeat-tp (once-td-tp f)))
  ([f z]
   ((outermost f) z)))

(def topdown full-td-tp)

(def bottomup full-bu-tp)

;; Type Unifying

(defn- into-array-list
  ([] (ArrayList.))
  ([x] (vec x))
  ([^List x ^List y]
   (if y
     (doto x
       (.addAll y))
     x)))

(defn- full-td-tu
  ([f m]
   (fn self [z]
     (let [top z]
       (loop [z z
              acc (m)]
         (let [acc (if-some [x (f z)]
                     (m acc x)
                     acc)]
           (if-let [z (znext-no-edit z top)]
             (recur z acc)
             acc))))))
  ([f m z]
   ((full-td-tu f m) z)))

(defn- stop-td-tu
  ([f m]
   (fn self [z]
     (let [top z]
       (loop [z z
              acc (m)]
         (let [x (f z)
               stop? (some? x)
               acc (if stop?
                     (m acc x)
                     acc)]
           (if-let [z (if stop?
                        (zright-or-up-no-edit z top)
                        (znext-no-edit z top))]
             (recur z acc)
             acc))))))
  ([f m z]
   ((stop-td-tu f m) z)))

(defn collect
  ([f z]
   (collect f into-array-list z))
  ([f m z]
   (m (full-td-tu f m z))))

(defn collect-stop
  ([f z]
   (collect-stop f into-array-list z))
  ([f m z]
   (m (stop-td-tu f m z))))
