(ns conduit
  (:use
     [clojure.contrib.seq-utils :only [indexed]]
     arrows))

(defn merge-parts [ps]
  (apply merge-with merge
         (map :parts ps)))

(defn conduit-seq-fn [l]
  (when (seq l)
    (fn conduit-seq [x]
      [[(first l)] (conduit-seq-fn (rest l))])))

(defn conduit-seq [l]
  "create a stream processor that emits the contents of a list
  regardless of what is fed to it"
  (let [new-fn (conduit-seq-fn l)]
    {:created-by :a-arr
     :reply {:fn new-fn}
     :no-reply {:fn new-fn}}))

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
         :parts (merge-parts [p1 p2])))

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
      (if (= f new-f)
        [new-x a-nth]
        [new-x (when new-f
                 (nth-fn n new-f))]))))

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

;; TODO: put under unit tests
(defn no-reply-par-fn [fs xs]
  (let [new-fs (map #(second (%1 %2)) fs xs)]
    [[] (partial no-reply-par-fn new-fs)]))

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
    (let [[new-x new-f] (f x)]
      [new-x (when new-f
               (partial select-fn 
                        (assoc selection-map v new-f)))])
    [[] (partial select-fn selection-map)]))

(defn no-reply-select-fn [selection-map [v x]]
  (let [v (if (contains? selection-map v)
            v
            '_)]
    (if-let [f (get selection-map v)]
      (let [[new-x new-f] (f x)]
        [new-x (partial no-reply-select-fn 
                        (assoc selection-map v new-f))])
      [[] (partial no-reply-select-fn selection-map)])))

(defn scatter-gather-select-fn [selection-map [v x]]
  (if-let [f (if (contains? selection-map v)
               (get selection-map v)
               (get selection-map '_))]
    (let [gather-fn (f x)]
      (fn []
        (let [[new-x new-f] (gather-fn)]
          [new-x (when new-f
                   (partial scatter-gather-select-fn 
                            (assoc selection-map v new-f)))])))
    [[] (partial scatter-gather-select-fn selection-map)]))

(defn map-vals [f m]
  (into {} (map (fn [[k v]]
                  [k (f v)])
                m)))

;; TODO: make this execute in a future
(defn scatter-gather-comp [f sg x]
  (let [[new-x new-f] (f x)]
    (if (empty? new-x)
      (fn []
        [[] (when new-f
              (partial scatter-gather-comp
                       new-f
                       sg))])
      (let [gather-fn (sg (first new-x))]
        (fn []
          (let [[newer-x new-sg] (gather-fn)]
            [newer-x (when (and new-f new-sg)
                       (partial scatter-gather-comp
                                new-f
                                new-sg))]))))))

(defn scatter-gather-nth [n sg x]
  (let [x (ensure-vec x)
        gather-fn (sg (nth x n))]
    (fn []
      (let [[y new-sg] (gather-fn)]
        (if (empty? y)
          [[] (when new-sg
                (partial scatter-gather-nth
                         n
                         new-sg))]
          [[(assoc x n (first y))]
           (when new-sg
             (scatter-gather-nth n new-sg))])))))

(defn a-par-scatter-gather [sgs x]
  (let [x (ensure-vec x)
        gather-fns (doall
                     (map #(%1 %2)
                          sgs
                          x))]

    (fn []
      (let [[new-xs new-sgs] (split-results
                               (map #(%) gather-fns))
            new-x (if (some empty? new-xs)
                    []
                    (vector (apply concat new-xs)))]
        [new-x
         (when (every? boolean new-sgs)
           (partial a-par-scatter-gather
                    new-sgs))])))) 

(defarrow conduit
          [a-arr (fn [f]
                   (let [new-fn (fn a-arr [x]
                                  [[(f x)] a-arr])
                         new-proc {:fn new-fn}]
                     {:created-by :a-arr
                      :args f
                      :no-reply new-proc
                      :reply new-proc
                      :scatter-gather {:fn (fn a-arr-sg [x]
                                             (fn []
                                               [[(f x)] a-arr-sg]))}}))

           a-comp (fn [& ps]
                    (let [first-ps (map :reply (butlast ps))
                          last-p (last ps)
                          p (reduce seq-proc first-ps)
                          reply-fn (:fn (seq-proc p (:reply last-p)))
                          no-reply-fn (:fn (seq-proc p (:no-reply last-p)))]
                      {:parts (merge-parts ps)
                       :created-by :a-comp
                       :args ps
                       :reply {:fn reply-fn}
                       :no-reply {:fn no-reply-fn}
                       :scatter-gather {:fn (partial scatter-gather-comp
                                                     (:fn p)
                                                     (get-in last-p [:scatter-gather :fn]))}}))
          
           a-nth (fn [n p]
                   {:created-by :a-nth
                    :args [n p]
                    :parts (:parts p)
                    :reply {:fn (nth-fn n (:fn (:reply p)))}
                    :no-reply {:fn (nth-fn n (:fn (:no-reply p)))}
                    :scatter-gather {:fn (partial scatter-gather-nth
                                                  n
                                                  (:scatter-gather p))}})
                   
           a-par (fn [& ps]
                   {:created-by :a-par
                    :args ps
                    :parts (merge-parts ps)
                    :reply {:fn (partial par-fn
                                 (map (comp :fn :scatter-gather)
                                      ps))}
                    :no-reply {:fn (partial no-reply-par-fn
                                            (map (comp :fn
                                                       :no-reply
                                                       (partial a-comp {}))
                                                 ps))}
                    :scatter-gather {:fn (partial a-par-scatter-gather
                                                  (map (comp :fn :scatter-gather)
                                                       ps))}})

           a-all (fn [& ps]
                   (assoc (a-comp (a-arr (partial repeat (count ps)))
                                  (apply a-par ps))
                          :created-by :a-all
                          :args ps))

           a-select (fn [& vp-pairs]
                      (let [pair-map (apply hash-map vp-pairs)]
                        {:created-by :a-select
                         :args pair-map
                         :parts (merge-parts (vals pair-map))
                         :reply {:fn (partial select-fn
                                              (map-vals (comp :fn :reply)
                                                        pair-map))}
                         :no-reply {:fn (partial no-reply-select-fn
                                                 (map-vals (comp :fn
                                                                 :no-reply
                                                                 (partial a-comp {})) 
                                                           pair-map))}
                         :scatter-gather {:fn (partial scatter-gather-select-fn
                                                       (map-vals (comp :fn :reply)
                                                                 pair-map))}}))

           a-loop (fn 
                    ([body-proc initial-value]
                     (let [new-fn (partial loop-fn
                                     (get-in body-proc [:reply :fn])
                                     initial-value)]
                       {:created-by :a-loop
                        :args [body-proc initial-value]
                        :parts (:parts body-proc)
                        :reply {:fn new-fn}
                        :no-reply {:fn new-fn}}))
                    ([body-proc initial-value feedback-proc]
                     (let [new-fn (partial loop-fn
                                           (get-in body-proc [:reply :fn])
                                           (get-in feedback-proc [:reply :fn])
                                           initial-value)]
                       {:created-by :a-loop
                        :args [body-proc initial-value feedback-proc]
                        :parts (:parts (merge-parts [body-proc
                                                     feedback-proc]))
                        :reply {:fn new-fn}
                        :no-reply {:fn new-fn}})))
           ])

(def a-arr (conduit :a-arr))
(def a-comp (conduit :a-comp))
(def a-nth (conduit :a-nth))
(def a-par (conduit :a-par))
(def a-all (conduit :a-all))
(def a-select (conduit :a-select))
(def a-loop (conduit :a-loop))

(def pass-through
  (a-arr identity))

(defn a-except [p catch-p]
  (a-comp
    {:created-by :a-arr
     :parts (:parts p)
     :reply {:fn (partial (fn a-except [f x]
                            (try
                              (let [[new-x new-f] (f x)]
                                [[['_ (first new-x)]]
                                 (partial a-except new-f)])
                              (catch Exception e
                                [[[Exception [e x]]]
                                 (partial a-except f)])))
                          (get-in p [:reply :fn]))}}
    (a-select
      Exception catch-p
      '_ pass-through)))

(defn conduit-map [p l]
  (if-not (seq l)
    (empty l)
    (run-proc (:fn (seq-proc (:reply (conduit-seq l))
                             (:no-reply p))))))

(defmacro def-arr [name args & body]
  `(def ~name (a-arr (fn ~name ~args ~@body))))

(defn conduit-proc [proc-fn]
  (let [new-fn (fn this-fn [x]
                 [(proc-fn x) this-fn])]
  {:created-by :a-arr
   :reply {:fn new-fn}
   :no-reply {:fn new-fn}}))

(defmacro def-proc [name args & body]
  `(def ~name (conduit-proc (fn ~name ~args ~@body))))

(defn disperse [p]
  (let [reply-proc (a-comp p pass-through)]
    {:created-by :disperse
     :args p
     :parts (:parts p)
     :reply {:fn (fn disperse [xs]
                   [[(conduit-map reply-proc xs)]
                    disperse])}
     :no-reply {:fn (fn disperse-final [xs]
                      (dorun (conduit-map p xs))
                      [[] disperse-final])}}))

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
                    :disperse (disperse (test-conduit (:args p))))]
    (select-keys test-proc [:fn])))

(defn test-conduit-fn [p]
  (comp first (:fn (test-conduit p))))

