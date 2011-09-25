(ns conduit.test.core
  #_(:require [conduit.require :as conduit])
  (:use conduit.core :reload-all)
  (:use clojure.test
        arrows.core))

(use-fixtures :each #_(fn [f] (println) (f)))

(defn test-list-iter [l]
  (fn [x]
    (if (empty? l)
      [nil abort-c]
      [(test-list-iter (rest l))
       (fn [c] (c (first l)))])))

(deftest test-a-run
  (testing "a-run"
    (testing "should ignore empty values"
      (is (= [1 2 3]
             (a-run (test-list-iter [[1] [] [2] [3] []])))))

    (testing "should handle the stream stopping"
      (is (= [1 2 3]
             (a-run (partial (fn this-fn [x y]
                               (if (< x 4)
                                 [(partial this-fn (inc x))
                                  (fn [c] (c [x]))]
                                 [nil abort-c]))
                             1)))))))

(deftest test-conduit-seq-fn
  (is (= [:a :b :c]
         (a-run (conduit-seq-fn [:a :b :c])))))

(def pl (a-arr inc))
(def t2 (a-arr (partial * 2)))
(def flt (fn this-fn [x]
           (if (odd? x)
             [this-fn abort-c]
             [this-fn (fn [c] (c [x]))])))

(deftest test-conduit-map
  (is (empty? (conduit-map pl [])))
  (is (empty? (conduit-map pl nil)))
  (is (= (range 10)
         (conduit-map pass-through
                      (range 10)))))

(deftest test-conduit-seq
  (is (= [:a :b :c]
         (conduit-map (conduit-seq [:a :b :c]) (range 0 5)))))

(deftest test-a-arr
  (is (= (range 1 6)
         (conduit-map pl (range 5))))
  (is (= [0 2 4]
         (conduit-map t2 [0 1 2])))
  (is (= [0 2 4]
         (conduit-map flt (range 5)))))

(deftest test-a-comp
  (let [ts (a-comp pl flt t2)]
    (is (= 12 (first (wait-for-reply ts 5))))
    (is (= [10]
           (conduit-map (a-comp flt pl t2) [5 3 4])))
    (is (= [12 8]
           (conduit-map ts [5 3 4])))))

(deftest test-a-nth
  (let [tf (a-nth 0 pl)
        tn (a-nth 1 (fn this-fn [x]
                      [this-fn (fn [c]
                                 (c [3]))]))]
    (is (= [[4 5]]
           (conduit-map tf [[3 5]])))

    (is (= [[15 3] [:bogus 3]]
           (conduit-map tn [[15 :bogus] [:bogus 15]])))
    
    (is (= [[:a 3 :c]]
           (conduit-map tn [[15] [:bogus] [:a :b :c]])))))

(deftest test-a-par
  (let [tp (a-par
            (conduit-seq [:a :b :c])
            pl
            t2)
        tp1 (a-par
             (conduit-seq [:a :b :c])
             pl
             (test-list-iter [[1] [] [2]]))]
    (is (= [[:a 4 10] [:b 4 10] [:c 4 10]]
           (conduit-map tp
                        [[99 3 5] [98 3 5] [97 3 5]])))
    (is (= [[:a 4 1] [:c 4 2]]
           (conduit-map tp1
                        [[99 3 5] [99 3 5] [99 3 5]])))))

(deftest test-a-all
  (let [ta (a-all pl t2)]
    (is (= [[7 12]]
           (conduit-map ta [6])))))

(deftest test-a-select
  (let [tc (a-select
            :oops (fn [x]
                    [nil abort-c])
            true pl
            false t2)]
    (is (= [9 6]
           (conduit-map tc [[:oops 83] [true 8] [:bogus 100] [false 3]]))))

  (let [tc (a-select
             :oops (fn [x]
                     [nil abort-c])
            true pl
            false t2
            '_ pass-through)]
    (is (= [9 100 6]
           (conduit-map tc [[:oops 83] [true 8] [:bogus 100] [false 3]])))))

(deftest test-a-loop
  (let [bp1 (a-arr (partial apply +))]
    (is (= [0 1 3 6 10 15 21]
           (conduit-map (a-loop bp1 0) (range 7)))))

  (let [inc-every-third (a-comp
                         (a-loop
                          (a-comp
                           (a-all
                            (a-arr first)
                            pass-through)
                           (a-select
                            3 (a-par
                               (a-arr (constantly 0))
                               (a-arr inc))
                            '_ (a-arr identity)))
                          1
                          (a-comp (a-arr first)
                                  (a-arr inc)))
                         (a-arr second))]

    (is (= [0 0 1 0 0 1 0 0 1]
           (conduit-map inc-every-third (repeat 9 0))))))

(deftest test-comp-par-loop
  (is (= [4 5 11 10 10 10 10]
         (conduit-map
          (a-comp
           (a-all
            (a-loop (a-arr (fn [[m x]] (max m x))) 0)
            (a-loop (a-arr (fn [[m x]] (min m x))) 100))
           (a-arr #(apply + %)))
          [2 3 9 1 4 5 2]))))

(deftest test-a-catch
  (let [te (a-arr (fn [x]
                    (when (even? x)
                      (throw (Exception. "An even int")))
                    (* 2 x)))
        x (a-arr (fn this-fn [x]
                   (if (zero? (mod x 3))
                     (throw (Exception. "Div by 3"))
                     3)))
        ty (a-catch te
                    (a-arr (fn [[e _]]
                             10)))
        tz (a-catch x
                    (a-arr (fn [[e _]]
                             15)))]
    (is (thrown? Exception
                 (conduit-map te (range 5))))

    (is (= [nil 2 nil 6 nil]
           (conduit-map (a-catch te
                                 (a-arr (constantly nil)))
                        (range 5))))

    (is (= [[10 15] [2 3] [10 3] [6 15] [10 3]]
           (conduit-map (a-all ty tz)
                        (range 5)))))
  (let [e1 (a-arr (fn [x] (throw (ArithmeticException.))))
        e2 (a-arr (fn [x] (throw (OutOfMemoryError.))))
        t (a-arr (constantly true))]
    (is (thrown? ArithmeticException
                 (conduit-map
                  (a-catch OutOfMemoryError
                           e1
                           t)
                  [1])))
    (is (first (conduit-map
                (a-catch ArithmeticException
                         e1
                         t)
                [1])))
    (is (first (conduit-map
                (a-catch OutOfMemoryError
                         e2
                         t)
                [1])))
    (is (first (conduit-map
                (a-catch Throwable
                         e1
                         t)
                [1])))
    (is (first (conduit-map
                (a-catch Throwable
                         e2
                         t)
                [1])))))

(deftest test-a-finally
  (let [main-count (atom 0)
        finally-count (atom 0)
        te (a-arr (fn [x]
                    (when (even? x)
                      (throw (Exception. "An even int")))
                    (swap! main-count inc)
                    (* 2 x)))
        x (a-arr (fn this-fn [x]
                   (when (zero? (mod x 3))
                     (swap! main-count inc)
                     (throw (Exception. "Div by 3")))
                   (* 10 x)))
        fin-fn (a-arr (fn [x]
                        (swap! finally-count inc)))
        tx (a-finally te fin-fn)
        ty (a-finally x fin-fn)
        tf (a-catch tx (a-arr (constantly nil)))
        tz (a-catch ty (a-arr (fn [[_ x]] x)))]
    (is (= [nil 2 nil 6 nil]
           (conduit-map tf (range 5))))
    (is (= 2 @main-count))
    (is (= 5 @finally-count))

    (reset! main-count 0)
    (is (= [[0 0] [10 10] [20 20] [3 3] [40 40] [50 50]]
           (conduit-map (a-comp (a-all tz tz)
                                pass-through)
                        (range 6))))
    (is (= 4 @main-count))))

(deftest test-disperse
    (let [make-and-dec (a-comp (a-arr range)
                               (disperse
                                 (a-arr dec)))]
      (is (= [[]
              [-1]
              [-1 0]
              [-1 0 1]
              [-1 0 1 2]
              [-1 0 1 2 3]]
             (conduit-map make-and-dec
                          (range 6))))))

(deftest test-text-conduit-fn
    (is (= 5 (first ((test-conduit-fn pl) 4)))))

(run-tests)
