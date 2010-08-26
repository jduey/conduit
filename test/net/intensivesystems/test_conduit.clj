(ns net.intensivesystems.test-conduit
  (:use net.intensivesystems.conduit :reload-all)
  (:use
     clojure.test
     net.intensivesystems.arrows))

(use-fixtures :each #_(fn [f] (println) (f)))

(defn test-list-iter [l]
  "create a stream processor that emits the contents of a list
  regardless of what is fed to it"
  (fn [x]
      [(first l)
       (when (seq (rest l))
         (test-list-iter (rest l)))]))

(deftest test-run-proc
         (is (= [1 2 3]
                (run-proc (test-list-iter [[1] [] [2] [3] []])))))

(deftest test-conduit-seq-fn
         (is (= [:a :b :c]
                (run-proc (conduit-seq-fn [:a :b :c])))))

(deftest test-conduit-seq
         (is (= [:a :b :c]
                (a-run (conduit-seq [:a :b :c])))))

(deftest test-seq-fn
         (let [tf1 (seq-fn (:fn (new-proc {} (fn this-fn [x]
                                               (if (even? x)
                                                 [[] this-fn]
                                                 [[x] this-fn]))))
                           (:fn (new-proc {} (fn this-fn [x]
                                               [[(dec x)] this-fn]))))
               tf2 (seq-fn (test-list-iter [[1] [] [2]])
                           (:fn (new-proc {} (fn this-fn [x]
                                               [[(dec x)] this-fn]))))]
           (is (= [[0] tf1] (tf1 1)))
           (is (= [0 2]
                  (run-proc (seq-fn (test-list-iter [[1] [2] [3]])
                                    tf1))))
           (is (= [0 1]
                  (run-proc tf2)))))

(deftest test-nth-fn
         (let [test-fn1 (nth-fn 0 (test-list-iter [[1] [2] [3]]))
               test-fn2 (nth-fn 0 (test-list-iter [[] [2] [3]]))]
           (is (= [[1 1] [2 2] [3 3]] 
                  (run-proc (seq-fn (test-list-iter [[[:a 1]] [[:b 2]] [[:c 3]]])
                                    test-fn1))))
           (is (= [[2 2] [3 3]] 
                  (run-proc (seq-fn (test-list-iter [[[:a 1]] [[:b 2]] [[:c 3]]])
                                    test-fn2))))))

(deftest test-par-fn
         (let [f1 (fn this-fn [x]
                    (fn [] [[(inc x)] this-fn]))
               f2 (fn this-fn [x]
                    (if (= 5 x)
                      (fn [] [[] this-fn])
                      (fn [] [[(dec x)] this-fn])))]
           (is (= [5 3] (ffirst (par-fn [f1 f2] [4 4]))))
           (is (= [[4 2] [9 7]]
                  (run-proc (seq-fn (test-list-iter [[[5 5]] [[3 3]] [[8 8]]])
                            (partial par-fn [f1 f2])))))))

(deftest test-scatter-gather-fn
         (let [p1 {:fn (fn this-fn [x]
                         [[(inc x)] this-fn])}
               p2 {:fn (fn this-fn [x]
                         [[(dec x)] this-fn])}
               p3 {:fn (test-list-iter (cycle [[:a] [:b] []]))}]
           (is (= [[1 -1] [2 0] [3 1] [4 2] [5 3] [6 4] [7 5]]
                  (run-proc (seq-fn (test-list-iter (map (fn [x]
                                                           [(repeat 2 x)])
                                                         (range 7)))
                                    (partial par-fn (map scatter-gather-fn [p1 p2]))))))
           (is (= [[1 :a] [2 :b] [4 :a] [5 :b] [7 :a]]
                  (run-proc (seq-fn (test-list-iter (map (fn [x]
                                                           [(repeat 2 x)])
                                                         (range 7)))
                            (partial par-fn (map scatter-gather-fn [p1 p3]))))))))

(deftest test-loop-fn
         (let [bf1 (fn this-fn [[px cx]]
                     [[(+ px cx)] this-fn])
               bf2 (fn this-fn [[px cx]]
                     (if (even? cx)
                       [[] this-fn]
                       [[(+ px cx)] this-fn]))]
           (is (= [0 1 3 6 10]
                  (run-proc (seq-fn (test-list-iter (map vector (range 5)))
                                    (partial loop-fn bf1 0) ))))
           (is (= [1 4 9 16]
                  (run-proc (seq-fn (test-list-iter (map vector (range 9)))
                                    (partial loop-fn bf2 0) ))))))

(with-arrow conduit
            (def pl (a-arr inc))
            (def t2 (a-arr (partial * 2)))
            (deftest test-a-arr 
                     (is (= [1 2 3]
                            (conduit-map pl [0 1 2])))
                     (is (= [0 2 4]
                            (conduit-map t2 [0 1 2]))))

            (deftest test-a-comp
                     (let [ts (a-comp pl t2)]
                       (is (= [12 8 10]
                              (conduit-map ts [5 3 4])))))
            
            (deftest test-a-nth
                     (let [tf (a-nth 0 pl)
                           tn (a-nth 1 {:fn (fn this-fn [x]
                                              [[3] this-fn])})]
                       (is (= [[4 5]]
                              (conduit-map tf [[3 5]])))

                       (is (= [[15 3] [:bogus 3]]
                              (conduit-map tn [[15 :bogus] [:bogus 15]])))))
            
            (deftest test-a-par
                     (let [tp (a-par
                                {:fn (test-list-iter [[:a] [:b] [:c]])}
                                pl
                                t2)
                           tp1 (a-par
                                 {:fn (test-list-iter [[:a] [:b] [:c]])}
                                 pl
                                 {:fn (test-list-iter [[1] [] [2]])})]
                       (is (= [[:a 4 10] [:b 4 10] [:c 4 10]]
                              (conduit-map tp [[99 3 5] [99 3 5] [99 3 5]])))
                       (is (= [[:a 4 1] [:c 4 2]]
                              (conduit-map tp1 [[99 3 5] [99 3 5] [99 3 5]])))))

            (deftest test-a-par-final
                     (let [tp (a-par-final
                                {:fn (test-list-iter [[:a] [:b] [:c]])}
                                pl
                                t2)
                           tp1 (a-par-final
                                 {:fn (test-list-iter [[:a] [:b] [:c]])}
                                 pl
                                 {:fn (test-list-iter [[1] [] [2]])})]
                       (is (= [[:a 4 10] [:b 4 10] [:c 4 10]]
                              (conduit-map tp [[99 3 5] [99 3 5] [99 3 5]])))
                       (is (= [[:a 4 1] [:c 4 2]]
                              (conduit-map tp1 [[99 3 5] [99 3 5] [99 3 5]])))))

            (deftest test-a-all
                     (let [ta (a-all pl t2)]
                       (is (= [[7 12]]
                              (conduit-map ta [6])))))
            
            (deftest test-a-all-final
                     (let [ta (a-all-final pl t2)]
                       (is (= [[7 12]]
                              (conduit-map ta [6])))))

            (deftest test-a-select
                     (let [tc (a-select
                                :oops {:fn (fn this-fn [x] [[] this-fn])} 
                                true pl
                                false t2)]
                       (is (= [9 6]
                              (conduit-map tc [[:oops 83] [true 8] [:bogus 100] [false 3]]))))

                     (let [tc (a-select
                                :oops {:fn (fn this-fn [x] [[] this-fn])} 
                                true pl
                                false t2
                                '_ pass-through)]
                       (is (= [9 100 6]
                              (conduit-map tc [[:oops 83] [true 8] [:bogus 100] [false 3]])))))

            (deftest test-a-select-final
                     (let [tc (a-select-final
                                :oops {:fn (fn this-fn [x] [[] this-fn])} 
                                true pl
                                false t2)]
                       (is (= [9 6]
                              (conduit-map tc [[:oops 83] [true 8] [:bogus 100] [false 3]]))))

                     (let [tc (a-select-final
                                :oops {:fn (fn this-fn [x] [[] this-fn])} 
                                true pl
                                false t2
                                '_ pass-through)]
                       (is (= [9 100 6]
                              (conduit-map tc [[:oops 83] [true 8] [:bogus 100] [false 3]])))))

            (deftest test-a-loop
                     (let [bp1 (a-arr (partial apply +))]
                       (is (= [0 1 3 6 10 15 21]
                              (conduit-map (a-loop bp1 0) (range 7))))))

(with-arrow conduit
    (def inc-every-third (a-comp
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
                           (a-arr second)))

    (is (= [0 0 1 0 0 1 0 0 1]
           (conduit-map inc-every-third (repeat 9 0)))))
            (deftest test-pass-through
                     (is (= (range 10)
                            (conduit-map pass-through (range 10)))))

            (deftest test-a-except
                     (let [te (a-arr (fn [x]
                                       (when (even? x)
                                         (throw (Exception. "An even int")))
                                       (* 2 x)))]
                       (is (thrown? Exception
                                    (conduit-map te (range 5))))

                       (is (= [nil 2 nil 6 nil]
                              (conduit-map (a-except te
                                                     (a-arr (constantly nil)))
                                           (range 5))))))
            )

(run-tests)
