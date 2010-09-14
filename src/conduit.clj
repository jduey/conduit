(ns conduit
  (:use
     [clojure.contrib.seq-utils :only [indexed]]
     arrows))

(defn conduit-proc [proc-fn]
  {:created-by :a-arr
   :fn (fn this-fn [x]
         [(proc-fn x) this-fn])})

(defn conduit-seq-fn [l]
  (when (seq l)
    (fn conduit-seq [x]
      [[(first l)] (conduit-seq-fn (rest l))])))

(defn conduit-seq [l]
  "create a stream processor that emits the contents of a list
  regardless of what is fed to it"
  {:fn (conduit-seq-fn l)})

(defn run-proc [f]
  "execute a stream processor function"
  (when f
    (let [[new-x new-f] (f nil)]
      (if (empty? new-x)
        (recur new-f)
        (lazy-seq
          (cons (first new-x)
                (run-proc new-f)))))))

(defn a-run [p]
  (run-proc (:fn p)))

(defn seq-fn [f1 f2]
  "Link two processor functions together so that the output
  of the first is fed into the second. Returns a function."
  (cond
    (nil? f1) f2
    (nil? f2) f1
    :else (fn a-comp [x]
            (let [[x1 new1] (f1 x)
                  [x2 new2] (if (empty? x1)
                              [x1 f2]
                              (f2 (first x1)))]
              (cond
                (and (= f1 new1) (= f2 new2))
                [x2 a-comp]

                (or (not new1) (not new2))
                [x2 nil]

                :else
                [x2 (seq-fn new1 new2)])))))

(defmulti seq-proc (fn [p1 p2] (:type p2)))

(defmethod seq-proc :default [p1 p2]
  "compose two stream processors together sequentially"
  (assoc p1
         :fn (seq-fn (:fn p1) (:fn p2))
         :parts (merge-with merge
                            (:parts p1)
                            (:parts p2))))

(defn ensure-vec [x-vec]
  (if (vector? x-vec)
    x-vec
    (vec x-vec)))

(defn nth-fn [n f]
  "creates a processor function that always applies a function to the
  nth element of a given input vector, returning a new vector"
  (fn a-nth [x]
    (let [x (ensure-vec x)
          [y new-f] (f (nth x n))
          new-x (if (empty? y)
                  y
                  [(assoc x n (first y))])]
      (cond
        (nil? new-f) [new-x nil]
        (= f new-f) [new-x a-nth]
        :else [new-x (nth-fn n new-f)]))))

(defmulti scatter-gather-fn (fn [p] (:type p)))

(defn proc-fn-future [f x]
  (fn []
    (let [[new-x new-f] (f x)]
      [new-x (when new-f
               (partial proc-fn-future new-f))])))

(defmethod scatter-gather-fn :default [p]
  (partial proc-fn-future (:fn p)))

(defn split-results [results]
  (reduce (fn [[new-xs new-fs] [new-x new-f]]
            [(conj new-xs new-x)
             (conj new-fs new-f)])
          [[] []]
          results))

(defn par-fn [fs xs]
  (let [result-fns (doall (map #(%1 %2) fs xs))
        [new-xs new-fs] (split-results (map #(%) result-fns))
        new-x (if (some empty? new-xs)
                []
                (vector (apply concat new-xs)))]
    [new-x
     (when (every? boolean new-fs)
       (partial par-fn new-fs))]))

(defn loop-fn
  ([body-fn prev-x curr-x]
   (let [[new-x new-f] (body-fn [prev-x curr-x])]
     [new-x
      (cond
        (nil? new-f) nil
        (empty? new-x) (partial loop-fn new-f prev-x)
        :else (partial loop-fn new-f (first new-x)))]))
  ([body-fn feedback-fn prev-x curr-x]
   (let [[new-x new-f] (body-fn [prev-x curr-x])
         [fb-x new-fb-f] (if-not (empty? new-x)
                           (feedback-fn (first new-x)))]
     [new-x
      (cond
        (nil? new-f) nil
        (empty? new-x) (partial loop-fn new-f feedback-fn prev-x)
        (nil? new-fb-f) nil
        (empty? fb-x) (partial loop-fn new-f new-fb-f prev-x)
        :else (partial loop-fn new-f new-fb-f (first fb-x)))])))

(defn select-fn [selection-map [v x]]
  (if-let [f (if (contains? selection-map v)
               (get selection-map v)
               (get selection-map '_))]
    (let [[new-x new-f] ((f x))]
      [new-x (when new-f
               (partial select-fn 
                        (assoc selection-map v new-f)))])
    [[] (partial select-fn selection-map)]))

(defn map-vals [f m]
  (into {} (map (fn [[k v]]
                  [k (f v)])
                m)))

(defmulti reply-proc (fn this-bogus-fn [p] (:type p)))

(defmethod reply-proc :default [p]
  p)

(defarrow conduit
          [a-arr (fn [f]
                   {:created-by :a-arr
                    :args f
                    :fn (fn a-arr [x]
                          [[(f x)] a-arr])})

           a-comp (fn [& ps]
                    (let [p (reduce seq-proc ps)]
                      {:fn (:fn p)
                       :parts (:parts p)
                       :created-by :a-comp
                       :args ps}))
          
           a-nth (fn [n p]
                   {:fn (nth-fn n (:fn p))
                    :created-by :a-nth
                    :args p
                    :parts (:parts p)})
                   
           a-par (fn [& ps]
                   (let [rp (map reply-proc ps)]
                     {:created-by :a-par
                      :args ps
                      :parts (apply merge-with merge
                                    (map :parts rp))
                      :fn (partial par-fn
                                   (map scatter-gather-fn rp))}))

           a-all (fn [& ps]
                   (assoc (a-comp (a-arr (partial repeat (count ps)))
                                  (apply a-par ps))
                          :created-by :a-all
                          :args ps))

           a-select (fn [& vp-pairs]
                      (let [pair-map (apply hash-map vp-pairs)]
                        {:created-by :a-select
                         :args pair-map
                         :parts (apply merge-with merge
                                       (map (comp :parts reply-proc)
                                            (vals pair-map)))
                         :fn (partial select-fn
                                      (map-vals scatter-gather-fn
                                                pair-map))}))

           a-loop (fn 
                    ([body-proc initial-value]
                     {:created-by :a-loop
                      :args [body-proc initial-value]
                      :parts (:parts body-proc)
                      :fn (partial loop-fn (:fn body-proc) initial-value)})
                    ([body-proc initial-value feedback-proc]
                     {:created-by :a-loop
                      :args [body-proc initial-value feedback-proc]
                      :parts (:parts (merge-with merge
                                                 (:parts body-proc)
                                                 (:parts feedback-proc)))
                      :fn (partial loop-fn (:fn body-proc)
                                   (:fn feedback-proc)
                                   initial-value)}))
           ])

(def a-arr (conduit :a-arr))
(def a-comp (conduit :a-comp))
(def a-nth (conduit :a-nth))
(def a-par (conduit :a-par))
(def a-all (conduit :a-all))
(def a-select (conduit :a-select))
(def a-loop (conduit :a-loop))

(defn conduit-map [p l]
  (if-not (seq l)
    (empty l)
    (a-run
      (seq-proc (conduit-seq l)
                p))))

(defmacro def-arr [name args & body]
  `(def ~name (~'a-arr (fn ~name ~args ~@body))))

(defmacro def-proc [name args & body]
  `(def ~name (conduit-proc (fn ~name ~args ~@body))))

(defn disperse [p]
  {:fn (fn disperse [xs]
         [[(conduit-map (reply-proc p)
                        xs)]
          disperse])
   :created-by :disperse
   :args p
   :parts (:parts p)})

(defn disperse-final [p]
  {:fn (fn disperse-final [xs]
         (dorun (conduit-map p xs))
         [[] disperse-final])
   :created-by :disperse-final
   :args p
   :parts (:parts p)})

(defn final-par-fn [fs xs]
  (let [[new-xs new-fs] (split-results (map #(%1 %2) fs xs))
        new-x (if (some empty? new-xs)
                []
                [(apply concat new-xs)])]
    [new-x
     (partial final-par-fn new-fs)]))

(defn a-par-final [& ps]
  {:parts (apply merge-with merge
                 (map :parts ps))
   :fn (partial final-par-fn
                (map (comp :fn
                           (partial a-comp {}))
                     ps))})

(defn a-all-final [& ps]
  (a-comp (a-arr (partial repeat (count ps)))
          (apply a-par-final ps)))

(defn final-select-fn [selection-map [v x]]
  (let [v (if (contains? selection-map v)
            v
            '_)]
    (if-let [f (get selection-map v)]
      (let [[new-x new-f] (f x)]
        [new-x (partial final-select-fn 
                        (assoc selection-map v new-f))])
      [[] (partial final-select-fn selection-map)])))

(defn a-select-final [& vp-pairs]
  (let [pair-map (apply hash-map vp-pairs)
        selection-map (map-vals (comp :fn
                                      (partial a-comp {})) 
                                pair-map)]
    {:parts (apply merge-with merge
                   (map :parts (vals pair-map)))
     :fn (partial final-select-fn selection-map)}))

(def pass-through
  (a-arr identity))

(defn test-conduit [p]
  (let [test-proc (condp = (:created-by p)
                    :a-arr p
                    :a-comp (apply a-comp (map test-conduit (:args p)))
                    :a-par (apply a-par (map test-conduit (:args p)))
                    :a-all (apply a-all (map test-conduit (:args p)))
                    :a-select (apply a-select (mapcat (fn [[k v]]
                                                        [k (test-conduit v)])
                                                      (:args p)))
                    :a-loop (let [[bp iv fb] (:args p)]
                              (if fb
                                (a-loop (test-conduit bp)
                                        iv
                                        (test-conduit fb))
                                (a-loop (test-conduit bp)
                                        iv)))
                    :disperse (disperse (test-conduit (:args p)))
                    :disperse-final (disperse-final (test-conduit (:args p))))]
    (select-keys test-proc [:fn])))

(defn test-conduit-fn [p]
  (comp first (:fn (test-conduit p))))

(defmacro a-except [p catch-p]
  `(~'a-comp
       {:fn (partial (fn a-except# [f# x#]
                            (try
                              (let [[new-x# new-f#] (f# x#)]
                                [[['_ (first new-x#)]]
                                 (partial a-except# new-f#)])
                              (catch Exception e#
                                [[[Exception [e# x#]]]
                                 (partial a-except# f#)])))
                          (:fn ~p))
        :created-by :a-arr
        :parts (:parts ~p)}
       (~'a-select
           Exception ~catch-p
           '_ pass-through)))

