(ns net.intensivesystems.conduit-samples
  (:use net.intensivesystems.conduit :reload-all)
  (:use
     clojure.test
     net.intensivesystems.arrows))

;; conduit-seq
(def ints (conduit-seq (range 5)))

(is (= [0 1 2 3 4]
      (a-run ints)))

;; a-arr
(with-arrow conduit
   (def conduit-inc (a-arr inc)))

;; a-comp
(with-arrow conduit
   (def inc-ints (a-comp ints conduit-inc)))

(is (= [1 2 3 4 5]
       (a-run inc-ints)))

;; a-all
(with-arrow conduit
   (def conduit-dec (a-arr dec))

   (def inc-n-dec (a-all conduit-inc
                         conduit-dec)))

(is (= [[1 -1] [2 0] [3 1] [4 2] [5 3]]
       (with-arrow conduit
                   (a-run (a-comp ints inc-n-dec)))))

;; conduit-map
(is (= [[1 -1] [2 0] [3 1] [4 2] [5 3]]
       (conduit-map inc-n-dec
                    (range 5))))

;; a-par
(with-arrow conduit
   (def par-inc-n-dec (a-par conduit-inc
                             conduit-dec)))

(is (= [[2 9] [4 14] [6 17] [9 11]]
       (conduit-map par-inc-n-dec
                    [[1 10] [3 15] [5 18] [8 12]])))

;; a-all constructed from a-par
(with-arrow conduit
   (def new-inc-n-dec (a-comp (a-arr (partial repeat 2))
                             par-inc-n-dec)))

(is (= [[1 -1] [2 0] [3 1] [4 2] [5 3]]
       (conduit-map new-inc-n-dec
                    (range 5))))

;; a-nth
(with-arrow conduit
   (def inc-first (a-nth 0 conduit-inc)))

(is (= [[13 9] [9 :a] [100 8] [22 :b]]
       (conduit-map inc-first
                    [[12 9] [8 :a] [99 8] [21 :b]])))

;; a-select
(with-arrow conduit
   (def inc-odd-dec-even (a-select
                            :odd conduit-inc
                            :even conduit-dec)))

(is (= [2 1 6 8]
    (conduit-map inc-odd-dec-even [[:odd 1] [:even 2] [:odd 5] [:odd 7]])))

(with-arrow conduit
   (def pass-it-on (a-select
                      '_ (a-arr identity)
                      :odd conduit-inc
                      :even conduit-dec)))

(is (= [2 1 5 7]
    (conduit-map pass-it-on [[:odd 1] [:even 2] [:bogus 5] [:oops 7]])))

(with-arrow conduit
   (def discarding (a-select
                      :odd conduit-inc
                      :even conduit-dec)))

(is (= [2 1]
    (conduit-map discarding [[:odd 1] [:even 2] [:bogus 5] [:oops 7]])))

;; a-loop
(with-arrow conduit
    (def accumulate (a-loop
                      (a-arr (partial apply +))
                      0))

    (is (= [0 1 3 6 10 15 21]
           (conduit-map accumulate (range 7)))))
     
(with-arrow conduit
    (def extract-state-value (a-all
                                 (a-arr first)
                                 (a-arr identity)))

    (def inc-reset-state (a-par
                           (a-arr (constantly 0))
                           conduit-inc))

    (def inc-on-3 (a-select
                    3 inc-reset-state
                    '_ (a-arr identity)))
            
    (def inc-on-3-with-state (a-comp
                               extract-state-value
                               inc-on-3))

    (def update-state (a-comp (a-arr first)
                             conduit-inc))
            
    (def inc-every-third (a-comp
                           (a-loop
                             inc-on-3-with-state
                             1
                             update-state)
                           (a-arr second)))

    (is (= [0 0 1 0 0 1 0 0 1]
           (conduit-map inc-every-third (repeat 9 0)))))
