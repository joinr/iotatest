(ns iotatest.core
  (:require [iota :as iota]
            [tech.ml.dataset :as ds]
            [clj-memory-meter.core :as mm]
            [criterium.core :as c]
            [tech.v2.datatype :as dtype])
  (:import [it.unimi.dsi.fastutil.longs LongArrayList]
           [tech.v2.datatype ObjectReader]))

;;can I have a function that takes an iota vector,
;;and wraps a lazy string?
;;We're just storing the line number and offset
;;in the string.

(def bom (int \tab))
;;dumb parsing example.
(defn tsv->offsets [^String line]
  (let [boms (.getBytes line)
        bound (alength boms)]
    (loop [idx 0
           l   0
           r   0
           acc []]
      (cond (== idx bound)
              (conj acc [l (dec bound)])
            (== (aget boms idx) bom)
              (let [nxt (unchecked-inc idx)]
                (recur nxt
                       nxt
                       nxt
                       (conj acc [l (unchecked-dec r)])))
            :else
            (recur (unchecked-inc idx)
                   l
                   (unchecked-inc r)
                   acc)))))

(defn raw-entries [indices line]
  (let [offs (tsv->offsets line)]
    (mapv (fn [idx] (nth offs idx)) indices)))

(defn raw-columns [indices lines]
  (map #(raw-entries indices %) lines))

(defn text-reader
  (^ObjectReader [^LongArrayList  indices ^iota.FileVector backing ^clojure.lang.Atom cursor]
   (let [bound          (/ (count indices) 2)
         load-row!      (fn [idx]
                          (let [v (.deref ^clojure.lang.Atom cursor)]
                            (if (== ^long (v :idx) ^long idx)
                              v
                              (reset! cursor {:idx idx
                                              :line (nth backing idx)}))))]
     (reify ObjectReader
       (lsize [rdr] bound)
       (read [rdr idx]
         (let [off-idx (* 2 idx)
               l       (.get indices  off-idx)
               r       (.get indices (inc off-idx))
               line    ((load-row! idx) :line)]
           (.subSequence ^String line l (inc r))))
       clojure.lang.Indexed
       (nth [this idx] (.read this idx))
       (nth [this idx not-found]
         (if (and (not (neg? idx))
                  (<= idx bound))
           (.read this idx) not-found))
       ))))

(defn text-columns [indices ^iota.FileVector lines]
  (let [bound (count indices)
        cursor  (atom {:idx -1})
        ^clojure.lang.Indexed cols
        (vec (repeatedly bound #(LongArrayList.)))]
    (doseq [^clojure.lang.Indexed row (raw-columns indices lines)]
      (dotimes [n bound]
        (let [^clojure.lang.Indexed entry (.nth row n)
              ^LongArrayList col  (.nth cols n)
              ^long l    (.nth entry 0)
              ^long r (.nth entry 1)]
          (.add col l)
          (.add col r))))
    (mapv (fn [col]
            (text-reader col lines cursor)) cols)))

(defn indexless-text-columns
  (^ObjectReader [columns ^iota.FileVector backing]
   (let [col-count      (count columns)
         bound          (count backing)
         cursor         (atom {:idx -1})
         load-row!      (fn [idx]
                          (let [v (.deref ^clojure.lang.Atom cursor)]
                            (if (== ^long (v :idx) ^long idx)
                              v
                              (let [line (nth backing idx)]
                                (reset! cursor {:idx idx
                                                :line line
                                                :row  (raw-entries columns line)})))))]
     (reify ObjectReader
       (lsize [rdr] col-count)
       (read [rdr col-idx]
         (dtype/object-reader
          bound
          (fn [^long idx]
            (let [state (load-row! idx)
                  row   (state :row)
                  line  (state :line)
                  lr    (row col-idx)]
              (.subSequence ^String line (lr 0) (inc (lr 1)))))))
       clojure.lang.Indexed
       (nth [this idx] (.read this idx))
       (nth [this idx not-found]
         (if (and (not (neg? idx))
                  (<= idx bound))
           (.read this idx) not-found))
       ))))




(comment ;testing

  (def bigv (iota/vec "../../sampledata.txt"))
  (def text-cols  (text-columns [0 9] bigv))
  (def text-cols-less  (indexless-text-columns [0 9] bigv))
  (def big-ds (ds/->dataset "../../sampledata.txt"))

  (mm/measure big-ds)
  ;;"94.0 MB"

  (mm/measure (big-ds "DemandGroup"))
  ;;"3.4 MB"

  (mm/measure text-cols)
  ;;no compression, don't need the address space...blech
  ;;"95.7 MB"

  ;;just a thin facade around iota, with some row caching.
  (mm/measure text-cols-less)
  ;;"1.8 MB"

  (->>  [(big-ds "DemandGroup")
         (second text-cols-less)
         (second text-cols)]
        (map  #(nth % 100000))
        (every? #{"Molly"}))
  ;;true

  (let [col (big-ds "DemandGroup")]
    (c/quick-bench (nth col 100000)))
  ;; Evaluation count : 25446888 in 6 samples of 4241148 calls.
  ;; Execution time mean : 21.809562 ns

  ;;probably crappy implementation, didn't bother to optimize
  ;;much.
  (let [col (second text-cols)]
    (c/quick-bench (nth col 100000)))

  ;;Evaluation count : 11442594 in 6 samples of 1907099 calls.
  ;;Execution time mean : 50.833021 ns

  (let [col (second text-cols-less)]
    (c/quick-bench (nth col 100000)))
  ;;Evaluation count : 9556044 in 6 samples of 1592674 calls.
  ;;Execution time mean : 64.704866 ns



  )
