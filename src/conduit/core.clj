(ns conduit.core
  (:use [clojure.contrib.seq-utils :only [indexed]]
        [clojure.pprint :only [pprint]]
        arrows.core))

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
    {:reply new-fn
     :no-reply new-fn
     :scatter-gather (fn [x]
                       (partial new-fn x))}))

(defn a-run [f]
  "execute a stream processor function"
  (when f
    (let [[new-x new-f] (f nil)]
      (if (empty? new-x)
        (recur new-f)
        (lazy-seq
         (cons (first new-x)
               (a-run new-f)))))))

(defn comp-fn [f1 f2]
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
              (or (not new1) (not new2))
              [x2 nil]

              (and (= f1 new1) (= f2 new2))
              [x2 a-comp]

              :else
              [x2 (comp-fn new1 new2)])))))

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

(defn sg-par-fn [fs xs]
  (let [result-fns (doall (map #(%1 %2) fs xs))
        [new-xs new-fs] (split-results (map #(%) result-fns))
        new-x (if (some empty? new-xs)
                []
                (vector (apply concat new-xs)))]
    [new-x
     (when (every? boolean new-fs)
       (partial sg-par-fn new-fs))]))

;; TODO: put under unit test
(defn par-fn [fs xs]
  (let [[new-xs new-fs] (split-results (map #(%1 %2) fs xs))
        new-x (if (some empty? new-xs)
                []
                (vector (apply concat new-xs)))]
    [new-x (partial par-fn new-fs)]))

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

(defn sg-loop-fn
  ([body-fn prev-x curr-x]
     (let [gather-fn (body-fn [prev-x curr-x])]
       (fn []
         (let [[new-x new-f] (gather-fn)]
           [new-x
            (cond
             (nil? new-f) nil
             (empty? new-x) (partial sg-loop-fn new-f prev-x)
             :else (partial sg-loop-fn new-f (first new-x)))]))))
  ([body-fn feedback-fn prev-x curr-x]
     (let [gather-fn (body-fn [prev-x curr-x])]
       (fn []
         (let [[new-x new-f] (gather-fn)
               [fb-x new-fb-f] (if-not (empty? new-x)
                                 (feedback-fn (first new-x)))]
           [new-x
            (cond
             (nil? new-f) nil
             (empty? new-x) (partial sg-loop-fn new-f feedback-fn prev-x)
             (nil? new-fb-f) nil
             (empty? fb-x) (partial sg-loop-fn new-f new-fb-f prev-x)
             :else (partial sg-loop-fn new-f new-fb-f (first fb-x)))])))))

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
                 new-proc new-fn]
             {:created-by :a-arr
              :args f
              :no-reply new-proc
              :reply new-proc
              :scatter-gather (fn a-arr-sg [x]
                                (fn []
                                  [[(f x)] a-arr-sg]))}))

   a-comp (fn [& ps]
            (let [first-ps (map :reply (butlast ps))
                  last-p (last ps)
                  p (reduce comp-fn first-ps)
                  reply-fn (comp-fn p (:reply last-p))
                  no-reply-fn (comp-fn p (:no-reply last-p))]
              {:parts (merge-parts ps)
               :created-by :a-comp
               :args ps
               :reply reply-fn
               :no-reply no-reply-fn
               :scatter-gather (partial scatter-gather-comp
                                        p
                                        (:scatter-gather last-p))}))

   a-nth (fn [n p]
           {:created-by :a-nth
            :args [n p]
            :parts (:parts p)
            :reply (nth-fn n (:reply p))
            :no-reply (nth-fn n (:no-reply p))
            :scatter-gather (partial scatter-gather-nth
                                     n
                                     (:scatter-gather p))})

   a-par (fn [& ps]
           {:created-by :a-par
            :args ps
            :parts (merge-parts ps)
            :reply (partial sg-par-fn
                            (map :scatter-gather ps))
            :no-reply (partial par-fn
                               (map (comp :no-reply
                                          (partial a-comp {}))
                                    ps))
            :scatter-gather (partial a-par-scatter-gather
                                     (map :scatter-gather ps))})

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
                 :reply (partial select-fn
                                 (map-vals :reply pair-map))
                 :no-reply (partial no-reply-select-fn
                                    (map-vals (comp :no-reply
                                                    (partial a-comp {}))
                                              pair-map))
                 :scatter-gather (partial scatter-gather-select-fn
                                          (map-vals :reply pair-map))}))

   a-loop (fn
            ([body-proc initial-value]
               (let [new-fn (partial loop-fn
                                     (:reply body-proc)
                                     initial-value)
                     sg-fn (partial sg-loop-fn
                                    (:scatter-gather body-proc)
                                    initial-value)]
                 {:created-by :a-loop
                  :args [body-proc initial-value]
                  :parts (:parts body-proc)
                  :reply new-fn
                  :no-reply new-fn
                  :scatter-gather sg-fn}))
            ([body-proc initial-value feedback-proc]
               (let [new-fn (partial loop-fn
                                     (:reply body-proc)
                                     (:reply feedback-proc)
                                     initial-value)]
                 {:created-by :a-loop
                  :args [body-proc initial-value feedback-proc]
                  :parts (:parts (merge-parts [body-proc
                                               feedback-proc]))
                  :reply new-fn
                  :no-reply new-fn})))
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

(defn a-selectp [pred & vp-pairs]
  (a-comp
   (a-all (a-arr pred)
          pass-through)
   (apply a-select vp-pairs)))

(defn a-if [a b c]
  (a-comp (a-all (a-arr (comp boolean a))
                 pass-through)
          (a-select
           true b
           false c)
          pass-through))

(defn a-except [p catch-p]
  (letfn [(a-except [f catch-f x]
                    (try
                      (let [[new-x new-f] (f x)]
                        [new-x (partial a-except new-f catch-f)])
                      (catch Exception e
                        (let [[new-x new-catch] (catch-f [e x])]
                          [new-x (partial a-except f new-catch)]))))]
    {:parts (:parts p)
     :reply (partial a-except
                     (:reply p)
                     (:reply catch-p))
     :no-reply (partial a-except
                        (:no-reply p)
                        (:no-reply catch-p))
     :created-by :a-except
     :args [p catch-p]}))

(defn a-finally [p final-p]
  (letfn [(a-finally [f final-f x]
                     (try
                       (let [[new-x new-f] (f x)]
                         [new-x (partial a-finally new-f final-f)])
                       (finally
                        (final-f x))))]
    {:parts (:parts p)
     :reply (partial a-finally
                     (:reply p)
                     (:reply final-p))
     :no-reply (partial a-finally
                        (:no-reply p)
                        (:no-reply final-p))
     :created-by :a-finally
     :args [p final-p]}))

(defn conduit-do [p & [v]]
  (a-arr (fn [x]
           ((:no-reply p) x)
           [v])))

(defn conduit-map [p l]
  (if-not (seq l)
    (empty l)
    (let [result (a-run (comp-fn (:reply (conduit-seq l))
                                 (:no-reply p)))]
      (pr-str result)
      result)))

(defmacro def-arr [name args & body]
  `(def ~name (a-arr (fn ~name ~args ~@body))))

(defn conduit-proc [proc-fn]
  (let [new-fn (fn this-fn [x]
                 [(proc-fn x) this-fn])]
    {:created-by :conduit-proc
     :args proc-fn
     :reply new-fn
     :no-reply new-fn
     :scatter-gather (fn [x]
                       (partial new-fn x))}))

(defmacro def-proc [name args & body]
  `(def ~name (conduit-proc (fn ~name ~args ~@body))))

(defn disperse [p]
  (let [reply-fn (fn disperse [xs]
                   (if (seq xs)
                     (let [new-x (a-run (comp-fn (conduit-seq-fn xs)
                                                 (:reply p)))]
                       [[new-x] disperse])
                     [[[]] disperse]))]
    {:created-by :disperse
     :args p
     :parts (:parts p)
     :reply reply-fn
     :no-reply (fn disperse-final [xs]
                 [[(doall (conduit-map p xs))]
                  disperse-final])
     :scatter-gather (fn [xs]
                       (partial reply-fn xs))}))

(defn test-conduit [p]
  (condp = (:created-by p)
      nil p
      :conduit-proc (conduit-proc (:args p))
      :a-arr (a-arr (:args p))
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
      :a-except (apply a-except (map test-conduit (:args p)))
      :a-finally (apply a-finally (map test-conduit (:args p)))
      :disperse (disperse (test-conduit (:args p)))))

(defn test-conduit-fn [p]
  (comp first (:reply (test-conduit p))))

(defn debug-proc [label p]
  (a-comp (a-arr (fn [x]
                   (println label "received:" x)
                   x))
          p
          (a-arr (fn [x]
                   (println label "produced:" x)
                   x))))
