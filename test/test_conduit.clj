(ns test-conduit
  (:use conduit :reload-all)
  (:use
     clojure.test
     arrows))

(use-fixtures :each #_(fn [f] (println) (f)))

(defn test-list-iter [l]
  "create a stream processor that emits the contents of a list
  regardless of what is fed to it"
  (fn [x]
      [(first l)
       (when (seq (rest l))
         (test-list-iter (rest l)))]))

(deftest test-run-proc
         (testing "run-proc"
                  (testing "should ignore empty values"
                           (is (= [1 2 3]
                                  (run-proc (test-list-iter [[1] [] [2] [3] []])))))

                  (testing "should handle the stream stopping"
                           (is (= [1 2 3]
                                  (run-proc (partial (fn this-fn [x y]
                                                       [[x] (when (< x 3)
                                                              (partial this-fn (inc x)))])
                                                     1)))))
                  
                  (testing "should handle the stream stopping with an empty value"
                           (is (= [1 2]
                                  (run-proc (partial (fn this-fn [x y]
                                                       (if (< x 3)
                                                         [[x] (partial this-fn (inc x))]
                                                         [[] nil]))
                                                     1)))))))

(deftest test-conduit-seq-fn
         (is (= [:a :b :c]
                (run-proc (conduit-seq-fn [:a :b :c])))))

(deftest test-conduit-seq
         (is (= [:a :b :c]
                (a-run (conduit-seq [:a :b :c])))))

(deftest test-seq-fn
         (testing "seq-fn"
                  (testing "should work properly"
                           (let [tf (seq-fn (:fn (new-proc {} (fn this-fn [x]
                                                                (if (even? x)
                                                                  [[] this-fn]
                                                                  [[x] this-fn]))))
                                            (:fn (new-proc {} (fn this-fn [x]
                                                                [[(dec x)] this-fn]))))]
                             (is (= [[0] tf] (tf 1)))
                             (is (= [0 2]
                                    (run-proc (seq-fn (test-list-iter [[1] [2] [3]])
                                                      tf))))))

                  (testing "should handle empty values"
                           (let [tf (seq-fn (test-list-iter [[1] [] [2]])
                                            (fn this-fn [x]
                                              [[(dec x)] this-fn]))]
                             (is (= [0 1]
                                    (run-proc tf)))))
         
                  (let [tf (seq-fn (test-list-iter [[1] [2] [3] [4] [5]])
                                   (fn this-fn [x]
                                     [[x] (when (< x 3)
                                            this-fn)]))]
                    (testing "should handle the first stream stopping"
                             (is (= [1 2 3]
                                    (run-proc tf))))

                    (testing "should handle the second stream stopping"
                             (is (= [2 3 4]
                                    (run-proc (seq-fn tf
                                                      (fn this-fn [x]
                                                        [[(inc x)] this-fn])))))))
                  
                  (let [tf (seq-fn (test-list-iter [[1] [2] [3] [4] [5]])
                                   (fn this-fn [x]
                                     (when (< x 3)
                                       [[x] this-fn])))]
                    (testing "should handle the first stream stopping with an empty value"
                             (is (= [1 2]
                                    (run-proc tf))))

                    (testing "should handle the second stream stopping with an empty value"
                             (is (= [2 3]
                                    (run-proc (seq-fn tf
                                                      (fn this-fn [x]
                                                        [[(inc x)] this-fn])))))))))

(deftest test-nth-fn
         (testing "nth-fn"
                  (testing "should work properly"
                           (let [tf (nth-fn 0 (test-list-iter [[1] [2] [3]]))]
                             (is (= [[1 1] [2 2] [3 3]] 
                                    (run-proc (seq-fn (test-list-iter [[[:a 1]] [[:b 2]] [[:c 3]]])
                                                      tf))))))

                  (testing "should handle empty values"
                           (let [tf (nth-fn 0 (test-list-iter [[] [2] [3]]))]
                             (is (= [[2 2] [3 3]] 
                                    (run-proc (seq-fn (test-list-iter [[[:a 1]] [[:b 2]] [[:c 3]]])
                                                      tf))))))

                  (testing "should handle the stream stopping"
                           (let [tf (nth-fn 0 (fn this-fn [x]
                                                [[3] (when (not= x :b)
                                                       this-fn)]))]
                             (is (= [[3 1] [3 2]] 
                                    (run-proc (seq-fn (test-list-iter [[[:a 1]] [[:b 2]] [[:c 3]]])
                                                      tf))))))

                  (testing "should handle the stream stopping with an empty value"
                           (let [tf (nth-fn 0 (fn this-fn [x]
                                                (when (not= x :c)
                                                       [[3] this-fn])))]
                             (is (= [[3 1] [3 2]] 
                                    (run-proc (seq-fn (test-list-iter [[[:a 1]] [[:b 2]] [[:c 3]]])
                                                      tf))))))))

(deftest test-par-fn
         (testing "par-fn"
                  (let [f1 (fn this-fn [x]
                             (fn []
                               [[(inc x)] this-fn]))
                        f2 (fn this-fn [x]
                             (if (= 5 x)
                               (fn [] [[] this-fn])
                               (fn [] [[(dec x)] this-fn])))
                        f3 (fn this-fn [x]
                             (fn []
                               [[x] (when (not= x 3)
                                      this-fn)]))
                        f4 (fn this-fn [x]
                             (fn []
                               (when (not= x 8)
                                 [[x] this-fn])))]
                    (testing "should work properly"
                             (is (= [[5 3]] (first (par-fn [f1 f2] [4 4])))))

                    (testing "should handle empty values"
                             (is (= [[4 2] [9 7]]
                                    (run-proc (seq-fn (test-list-iter [[[5 5]] [[3 3]] [[8 8]]])
                                                      (partial par-fn [f1 f2]))))))

                    (testing "should handle the first proc stopping"
                             (is (= [[6 5] [4 3]]
                                    (run-proc (seq-fn (test-list-iter [[[5 5]] [[3 3]] [[8 8]]])
                                                      (partial par-fn [f1 f3]))))))

                    (testing "should handle second proc stopping"
                             (is (= [[5 6] [3 4]]
                                    (run-proc (seq-fn (test-list-iter [[[5 5]] [[3 3]] [[8 8]]])
                                                      (partial par-fn [f3 f1]))))))

                    (testing "should handle the first proc stopping with an empty value"
                             (is (= [[6 5] [4 3]]
                                    (run-proc (seq-fn (test-list-iter [[[5 5]] [[3 3]] [[8 8]]])
                                                      (partial par-fn [f1 f4]))))))

                    (testing "should handle second proc stopping with an empty value"
                             (is (= [[5 6] [3 4]]
                                    (run-proc (seq-fn (test-list-iter [[[5 5]] [[3 3]] [[8 8]]])
                                                      (partial par-fn [f4 f1])))))))))

(deftest test-scatter-gather-fn
         (testing "scatter-gather-fn"
                  (let [p1 {:fn (fn this-fn [x]
                                  [[(inc x)] this-fn])}
                        p2 {:fn (fn this-fn [x]
                                  [[(dec x)] this-fn])}
                        p3 {:fn (test-list-iter (cycle [[:a] [:b] []]))}
                        p4 {:fn (fn this-fn [x]
                                  [[x] (when (not= x 4)
                                         this-fn)])}
                        p5 {:fn (fn this-fn [x]
                                  (when (not= x 5)
                                    [[x] this-fn]))}]
                    (testing "should work properly"
                             (is (= [[1 -1] [2 0] [3 1] [4 2] [5 3] [6 4] [7 5]]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [% %]) (range 7)))
                                                      (partial par-fn (map scatter-gather-fn [p1 p2])))))))

                    (testing "should handle empty values"
                             (is (= [[1 :a] [2 :b] [4 :a] [5 :b] [7 :a]]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [% %]) (range 7)))
                                                      (partial par-fn (map scatter-gather-fn [p1 p3])))))))

                    (testing "should handle a stream stopping"
                             (is (= [[1 0] [2 1] [3 2] [4 3] [5 4]]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [% %]) (range 7)))
                                                      (partial par-fn (map scatter-gather-fn [p1 p4])))))))

                    (testing "should handle a stream stopping with an empty value"
                             (is (= [[1 0] [2 1] [3 2] [4 3] [5 4]]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [% %]) (range 7)))
                                                      (partial par-fn (map scatter-gather-fn [p1 p5]))))))))))

(deftest test-loop-fn
         (testing "loop-fn"
                  (testing "should work as intended"
                           (let [tf (fn this-fn [[px cx]]
                                      [[(+ px cx)] this-fn])]
                             (is (= [0 1 3 6 10]
                                    (run-proc (seq-fn (test-list-iter (map vector (range 5)))
                                                      (partial loop-fn tf 0) ))))))

                  (testing "should handle empty values"
                           (let [tf (fn this-fn [[px cx]]
                                      (if (even? cx)
                                        [[] this-fn]
                                        [[(+ px cx)] this-fn]))]
                             (is (= [1 4 9 16]
                                    (run-proc (seq-fn (test-list-iter (map vector (range 9)))
                                                      (partial loop-fn tf 0) ))))))

                  (testing "should handle a stream ending"
                           (let [tf (fn this-fn [[px cx]]
                                      [[cx] (when (not= cx 6)
                                              this-fn)])]
                             (is (= (range 7)
                                    (run-proc (seq-fn (test-list-iter (map vector (range 9)))
                                                      (partial loop-fn tf 0)))))))
                  
                  (testing "should handle a stream ending with empty value"
                           (let [tf (fn this-fn [[px cx]]
                                      (if (not= cx 6)
                                        [[cx] this-fn]
                                        [[] nil]))]
                             (is (= (range 6)
                                    (run-proc (seq-fn (test-list-iter (map vector (range 9)))
                                                      (partial loop-fn tf 0))))))))
         (testing "loop-fn (with feedback function)"
                  (testing "should work as intended"
                           (let [tf (fn this-fn [[px cx]]
                                      [[(+ px cx)] this-fn])
                                 fb (fn this-fn [x]
                                      [[(inc x)] this-fn])]
                             (is (= [0 2 5 9 14]
                                    (run-proc (seq-fn (test-list-iter (map vector (range 5)))
                                                      (partial loop-fn tf fb 0) ))))))

                  (testing "should handle empty values"
                           (let [tf (fn this-fn [[px cx]]
                                      (if (even? cx)
                                        [[] this-fn]
                                        [[(+ px cx)] this-fn]))
                                 fb (fn this-fn [x]
                                      [[(inc x)] this-fn])]
                             (is (= [1 5 11 19]
                                    (run-proc (seq-fn (test-list-iter (map vector (range 9)))
                                                      (partial loop-fn tf fb 0) ))))))

                  (testing "should handle the stream ending"
                           (let [tf (fn this-fn [[px cx]]
                                      [[cx] (when (not= cx 6)
                                              this-fn)])
                                 fb (fn this-fn [x]
                                      [[(inc x)] this-fn])]
                             (is (= (range 7)
                                    (run-proc (seq-fn (test-list-iter (map vector (range 9)))
                                                      (partial loop-fn tf fb 0)))))))
                  
                  (testing "should handle the stream ending with empty value"
                           (let [tf (fn this-fn [[px cx]]
                                      (if (not= cx 6)
                                        [[cx] this-fn]
                                        [[] nil]))
                                 fb (fn this-fn [x]
                                      [[(inc x)] this-fn])]
                             (is (= (range 6)
                                    (run-proc (seq-fn (test-list-iter (map vector (range 9)))
                                                      (partial loop-fn tf 0)))))))

                  (testing "should handle the feedback stream ending"
                           (let [tf (fn this-fn [[px cx]]
                                      [[cx] this-fn])
                                 fb (fn this-fn [x]
                                      [[(inc x)] (when (not= x 6)
                                                   this-fn)])]
                             (is (= (range 7)
                                    (run-proc (seq-fn (test-list-iter (map vector (range 9)))
                                                      (partial loop-fn tf fb 0)))))))
                  
                  (testing "should handle the feedback stream ending with empty value"
                           (let [tf (fn this-fn [[px cx]]
                                      [[cx] this-fn])
                                 fb (fn this-fn [x]
                                      (when (not= x 6)
                                        [[(inc x)] this-fn]))]
                             (is (= (range 7)
                                    (run-proc (seq-fn (test-list-iter (map vector (range 9)))
                                                      (partial loop-fn tf fb 0)))))))))

(deftest test-select-fn
         (testing "select-fn"
                  (let [f1 (fn this-fn [x]
                             (fn []
                               [[(inc x)] this-fn]))
                        f2 (fn this-fn [x]
                             (fn []
                               [[(dec x)] this-fn]))
                        f3 (fn this-fn [x]
                             (fn []
                               [[] this-fn]))
                        f4 (fn this-fn [x]
                             (fn []
                               [[x] (when (not= x 5)
                                      this-fn)]))
                        f5 (fn this-fn [x]
                             (fn []
                               (when (not= x 6)
                                 [[x] this-fn])))]
                    (testing "should work properly"
                             (is (= [1 0 3 2 5 4]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [(even? %) %]) (range 6)))
                                                      (partial select-fn {true f1
                                                                          false f2}))))))

                    (testing "should use the default proc"
                             (is (= [1 0 3 2 5 4]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [(even? %) %]) (range 6)))
                                                      (partial select-fn {true f1
                                                                          '_ f2}))))))

                    (testing "should work when no match found"
                             (is (= [0 2 4]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [(even? %) %]) (range 6)))
                                                      (partial select-fn {:bogus f1
                                                                          false f2}))))))

                    (testing "should handle empty values from a proc"
                             (is (= [1 3 5]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [(even? %) %]) (range 6)))
                                                      (partial select-fn {true f1
                                                                          false f3}))))))

                    (testing "should handle a stream stopping"
                             (is (= [1 1 3 3 5 5]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [(even? %) %]) (range 10)))
                                                      (partial select-fn {true f1
                                                                          false f4}))))))

                    (testing "should handle a stream stopping with empty value"
                             (is (= [0 0 2 2 4 4]
                                    (run-proc (seq-fn (test-list-iter (map #(vector [(even? %) %]) (range 10)))
                                                      (partial select-fn {true f5
                                                                          false f2})))))))))

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

(deftest test-test-conduit
         (with-arrow conduit
                     (def-proc bogus [x]
                               [(inc x)]))

         (def tf (test-conduit bogus)))




(run-tests)
