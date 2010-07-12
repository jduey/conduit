(ns net.intensivesystems.conduit
  (:use
     [clojure.contrib.seq-utils :only [indexed]]
     net.intensivesystems.arrows))

(defn run-proc [f [first-x & rest-x]]
  "execute a stream processor function over a list of input values"
  (when (and f first-x)
    (let [[new-x new-f] (f first-x)]
      (if (empty? new-x)
        (recur new-f rest-x)
        (lazy-seq
          (cons (first new-x)
                (run-proc new-f rest-x)))))))

(defn a-run [p xs]
  (run-proc (:fn p) xs))

;; mutli-method to generate a new stream processor from a
;; template proc and a processor function
(defmulti new-proc (fn [p f] (:type p)))

(defmethod new-proc :default [p f]
  "default stream processor constructor"
  (assoc p :fn f))

(defn seq-fn [f1 f2]
  "Link two processor functions together so that the output
  of the first is fed into the second. Returns a function."
  (fn this-fn [x]
    (let [[x1 new1] (f1 x)
          [x2 new2] (if (empty? x1)
                      [x1 f2]
                      (f2 (first x1)))]
      (if (and (= f1 new1)
               (= f2 new2))
        [x2 this-fn]
        [x2 (seq-fn new1 new2)]))))

(defmulti seq-proc (fn [p1 p2] (:type p2)))

(defmethod seq-proc :default [p1 p2]
  "compose two stream processors together sequentially"
  (assoc p1 :fn (seq-fn (:fn p1) (:fn p2))))

(defn ensure-vec [x-vec]
  (if (vector? x-vec)
    x-vec
    (vec x-vec)))

(defn nth-fn [n f]
  "creates a processor function that always applies a function to the
  nth element of a given input vector, returning a new vector"
  (fn this-fn [x]
    (let [x (ensure-vec x)
          [y new-f] (f (nth x n))
          new-x (if (empty? y)
                  y
                  [(assoc x n (first y))])]
      (if (= f new-f)
        [new-x this-fn]
        [new-x (nth-fn n new-f)]))))

(defn deref-future [[xs fs] fut]
  (let [[new-x new-f] (deref fut)]
    [(conj xs new-x)
     (conj fs new-f)]))

(defn deref-futures [fut-list]
  (reduce deref-future
          [[] []]
          fut-list))

(defn par-fn [fs xs]
  (let [futs (doall (map #(% xs) fs))
        [new-xs new-fs] (deref-futures futs)
        new-x (if (some empty? new-xs)
                []
                [(apply concat new-xs)])]
    [new-x (partial par-fn new-fs)]))

(defmulti scatter-gather-fn (fn [ps] (:type (first ps))))

(defn proc-fn-future [f x]
  (future
    (let [[new-x new-f] (f x)]
      [new-x (partial proc-fn-future new-f)])))

(defmethod scatter-gather-fn :default [p]
  (partial proc-fn-future (:fn p)))

(defn nth-proc-fn [n f xs]
  (let [[new-x new-f] (f (nth xs n))]
    [new-x (partial nth-proc-fn n new-f)]))

(defn nth-procs [n p]
  (new-proc p (partial nth-proc-fn n (:fn p))))

(defn scatter-gather-fns [ps]
  (map scatter-gather-fn
       (map-indexed nth-procs ps)))

(defn loop-fn [body-fn prev-x curr-x]
  (let [[new-x new-f] (body-fn [prev-x curr-x])]
    (if (empty? new-x)
      [new-x (partial loop-fn new-f prev-x)]
      [new-x (partial loop-fn new-f (first new-x))])))

(def new-id (comp str gensym))

(defn select-fn [selection-map [v x]]
  (if-let [f (get selection-map v)]
    (let [[new-x new-f] (deref (f x))]
      [new-x (partial select-fn 
                      (assoc selection-map v new-f))])
    [[] (partial select-fn selection-map)]))

(defn map-vals [f m]
  (into {} (map f m)))

(defmulti conduit-run (fn ([p arg]
                           :default)
                        ([p arg & args]
                         (get-in p [:parts arg :type]))))

(defmethod conduit-run :default [p & args]
  (a-run p (first args)))

(defmulti reply-proc (fn [p] (:type p)))

(defmethod reply-proc :default [p]
  p)

(defarrow conduit
          [a-arr (fn [f]
                   (new-proc {} (fn this-fn [x]
                                  [[(f x)] this-fn])))

           a-seq (fn [& ps]
                   (reduce seq-proc ps))
          
           a-nth (fn [n p]
                   (new-proc p (nth-fn n (:fn p))))
                   
           a-par (fn [& ps]
                   {:parts (apply merge-with merge
                                  (map (comp :parts reply-proc) ps))
                   :fn (partial par-fn
                                 (scatter-gather-fns ps))})

           a-all (fn [& ps]
                   (a-seq (a-arr (partial repeat (count ps)))
                          (apply a-par ps)))

           a-select (fn [& vp-pairs]
                      (let [pair-map (apply hash-map vp-pairs)]
                        {:parts (apply merge-with merge
                                       (map (comp :parts reply-proc)
                                            (vals pair-map)))
                         :fn (partial select-fn
                                      (map-vals #(update-in % [1] scatter-gather-fn)
                                                pair-map))}))

           a-loop (fn [body-proc initial-value]
                    (new-proc body-proc
                              (partial loop-fn (:fn body-proc) initial-value)))
           ])

  
