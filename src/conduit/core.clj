(ns conduit.core
  (:use [clojure.contrib.seq-utils :only [indexed]]
        [clojure.contrib.def :only [defalias defmacro-]]
        [clojure.pprint :only [pprint]]
        [arrows.core]))

(defn merge-parts [ps]
  (apply merge-with merge
         (map :parts ps)))

(defn map-vals [f m]
  (into {} (map (fn [[k v]]
                  [k (f v)])
                m)))

(defn abort-c [c]
  (c []))

(defn conduit-seq-fn [l]
  (fn curr-fn [x]
    (let [new-f (conduit-seq-fn (rest l))]
      (if (empty? l)
        [nil abort-c]
        [new-f
         (fn [c]
           (c [(first l)]))]))))

(defn conduit-seq [l]
  "create a stream processor that emits the contents of a list
  regardless of what is fed to it"
  {:fn (conduit-seq-fn l)})

(defn a-run [f]
  "execute a stream processor function"
  (let [[new-f c] (f nil)
        y (c identity)]
    (cond
      (nil? new-f) (list)
      (empty? y) (recur new-f)
      :else (lazy-seq
              (cons (first y)
                    (a-run new-f))))))

#_(
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
   )

(defn wait-for-reply [{f :fn} x]
  ((second (f x)) identity))

(defn enqueue [{f :fn} & xs]
  (loop [[x & xs] xs
         f f]
    (when x
      (let [[new-f result-f] (f x)]
        (result-f nil)
        (recur xs new-f)))))

(defn comp-fn [[f & fs]]
  (fn curr-fn [x]
    (let [[new-f first-c] (f x)
          [new-fs new-c] (reduce (fn [[new-fs c] f]
                                   (let [y (c identity)
                                         [new-f new-c] (if (empty? y)
                                                         [f abort-c]
                                                         (f (first y)))]
                                     [(conj new-fs new-f) new-c]))
                                 [[new-f] first-c]
                                 fs)]
      [(when-not (some nil? new-fs)
         (comp-fn new-fs))
       (fn [c]
         (when c
           (new-c c)))])))

(defn nth-fn [n f]
  (fn curr-fn [xs]
    (if (<= (count xs) n)
      [curr-fn abort-c]
      (let [[new-f new-c] (f (nth xs n))
            y (new-c identity)]
        [(nth-fn n new-f)
         (if (empty? y)
           abort-c
           (fn [c]
             (when c
               (c [(assoc xs n (first y))]))))]))))

(defn par-fn [fs]
  (fn curr-fn [xs]
    (if (not= (count xs) (count fs))
      [curr-fn abort-c]
      (let [futs (doall
                   (map (fn [f x]
                          (future (let [[new-f c] (f x)
                                        y (c identity)]
                                    [new-f y])))
                        fs xs))
            fs-and-ys (map deref futs)
            [new-fs ys] (reduce (fn [[fs ys] [f y]]
                                  [(conj fs f) (conj ys y)])
                                [[] []]
                                fs-and-ys)]
        [(par-fn new-fs)
         (if (some empty? ys)
           abort-c
           (fn [c]
             (when c
               (c [(apply concat ys)]))))]))))

(defn select-fn [selection-map]
  (fn curr-fn [[v x]]
    (if-let [f (or (get selection-map v)
                   (get selection-map '_))]
      (let [[new-f c] (f x)]
        [(select-fn (assoc selection-map v new-f)) c])
      [curr-fn abort-c])))

(defn loop-fn
  ([f prev-x]
   (fn curr-fn [x]
     (let [[new-f c] (f [prev-x x])
           y (c identity)]
       (if (empty? y)
         [curr-fn abort-c]
         [(loop-fn new-f (first y)) (fn [c]
                                      (when c
                                        (c y)))]))))
  ([f fb-f prev-x]
   (fn curr-fn [x]
     (let [[new-f c] (f [prev-x x])
           y (c identity)]
       (if (empty? y)
         [curr-fn abort-c]
         (let [[new-fb fb-c] (fb-f (first y))
               fb-y (fb-c identity)]
           (if (empty? fb-y)
             [curr-fn abort-c]
             [(loop-fn new-f new-fb (first fb-y))
              (fn [c]
                (when c
                  (c y)))])))))))

(defarrow conduit
  [a-arr (fn [f]
           {:created-by :a-arr
            :args f
            :fn (fn a-arr [x]
                  (let [y (f x)]
                      [a-arr (fn [c]
                               (when c
                                 (c [y])))]))})

   a-comp (fn [& ps]
            {:parts (merge-parts ps)
             :created-by :a-comp
             :args ps
             :fn (if (< (count ps) 2)
                   (:fn (first ps))
                   (comp-fn (map :fn ps)))})

   a-nth (fn [n p]
           {:parts (:parts p)
            :created-by :a-nth
            :args [n p]
            :fn (nth-fn n (:fn p))})

   a-par (fn [& ps]
           {:created-by :a-par
            :args ps
            :parts (merge-parts ps)
            :fn (par-fn (map :fn ps))})

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
                 :fn (select-fn (map-vals :fn pair-map))}))

   a-loop (fn
            ([p initial-value]
             {:created-by :a-loop
              :args [p initial-value]
              :parts (:parts p)
              :fn (loop-fn (:fn p) initial-value)})
            ([p initial-value fb-p]
             {:created-by :a-loop
              :args [p initial-value fb-p]
              :parts (:parts p)
              :fn (loop-fn (:fn p) (:fn fb-p) initial-value)}))
   ])

(def a-arr (conduit :a-arr))
(def a-comp (conduit :a-comp))
(def a-nth (conduit :a-nth))
(def a-par (conduit :a-par))
(def a-all (conduit :a-all))
(def a-select (conduit :a-select))
(def a-loop (conduit :a-loop))

(defn conduit-map [p l]
  (if (empty? l)
    l
    (a-run (comp-fn [(:fn (conduit-seq l))
                     (:fn p)]))))

(def pass-through
  (a-arr identity))

(defn a-selectp [pred & vp-pairs]
  (a-comp
   (a-all (a-arr pred)
          pass-through)
   (apply a-select vp-pairs)))

(defn a-if [a b & [c]]
  (let [c (or c (a-arr (constantly nil)))]
    (a-comp (a-all (a-arr (comp boolean a))
                   pass-through)
            (a-select
             true b
             false c))))

(defn a-catch
  ([p catch-p]
     (a-catch Exception p catch-p))
  ([class p catch-p]
     (letfn [(a-catch [f catch-f]
               (fn [x]
                 (try
                   (let [[new-f c] (f x)]
                     [(a-catch f catch-f) c])
                   (catch Throwable e
                     (if (instance? class e)
                       (let [[new-catch c] (catch-f [e x])]
                         [(a-catch f new-catch) c])
                       (throw e))))))]
       {:parts (:parts p)
        :fn (a-catch (:fn p)
                     (:fn catch-p))
        :created-by :a-catch
        :args [class p catch-p]})))

(defn a-finally [p final-p]
  (letfn [(a-finally [f final-f x]
            (try
              (let [[new-f c] (f x)]
                [(a-finally f final-f) c])
              (finally
               (final-f x))))]
    {:parts (:parts p)
     :fn (a-finally (:fn p)
                    (:fn final-p))
     :created-by :a-finally
     :args [p final-p]}))

(defmacro def-arr [name args & body]
  `(def ~name (a-arr (fn ~name ~args ~@body))))

(defn a-filter [f]
  {:create-by :a-filter
   :args f
   :fn (fn curr-fn [x]
         (if (f x)
           [curr-fn (fn [c]
                      (when c
                        (c [x])))]))})

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

#_(
(defn test-conduit [p]
  (binding [*testing-conduit* true]
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
      :a-catch (apply a-catch (first (:args p))
                      (map test-conduit (rest (:args p))))
      :a-finally (apply a-finally (map test-conduit (:args p)))
      :disperse (disperse (test-conduit (:args p))))))

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
  )
