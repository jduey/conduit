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

(deftest test-constant-stream-fn
         (is (= [:a :b :c]
                (run-proc (constant-stream-fn [:a :b :c])))))

(deftest test-constant-stream
         (is (= [:a :b :c]
                (a-run (constant-stream [:a :b :c])))))

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
                            (conduit-run pl [0 1 2])))
                     (is (= [0 2 4]
                            (conduit-run t2 [0 1 2]))))

            (deftest test-a-seq
                     (let [ts (a-seq pl t2)]
                       (is (= [12 8 10]
                              (conduit-run ts [5 3 4])))))
            
            (deftest test-a-nth
                     (let [tf (a-nth 0 pl)
                           tn (a-nth 1 {:fn (fn this-fn [x]
                                              [[3] this-fn])})]
                       (is (= [[4 5]]
                              (conduit-run tf [[3 5]])))

                       (is (= [[15 3] [:bogus 3]]
                              (conduit-run tn [[15 :bogus] [:bogus 15]])))))
            
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
                              (conduit-run tp [[99 3 5] [99 3 5] [99 3 5]])))
                       (is (= [[:a 4 1] [:c 4 2]]
                              (conduit-run tp1 [[99 3 5] [99 3 5] [99 3 5]])))))
            
            (deftest test-a-all
                     (let [ta (a-all pl t2)]
                       (is (= [[7 12]]
                              (conduit-run ta [6])))))

            (deftest test-a-select
                     (let [tc (a-select
                                :oops {:fn (fn this-fn [x] [[] this-fn])}
                                true pl
                                false t2)]
                       (is (= [9 6]
                              (conduit-run tc [[:oops 83] [true 8] [:bogus 100] [false 3]])))))

            (deftest test-a-loop
                     (let [bp1 (a-arr (partial apply +))]
                       (is (= [0 1 3 6 10]
                              (conduit-run (a-loop bp1 0) (range 5))))))
            )

(run-tests)
