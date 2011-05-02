(ns conduit.require
  (:use [conduit.core :exclude [disperse test-conduit test-conduit-fn]])
  (:refer-clojure :exclude [case comp juxt reduce map identity]))

(def identity pass-through)
(def proc a-arr)
(def proc* conduit-proc)
(def case a-selectp)
(def comp a-comp)
(def split a-all)
(def juxt a-par)
(def reduce a-loop)
(def map conduit-map)
(def do conduit-do)
(def select a-select)
(def selectp a-selectp)
(def if a-if)
(def try a-except)
(def finally a-finally)
(def disperse conduit.core/disperse)
(def test-conduit conduit.core/test-conduit)
(def test-conduit-fn conduit.core/test-conduit-fn)
(defmacro def [name args & body]
  `(def ~name (a-arr (fn ~name ~args ~@body))))
(defmacro def* [name args & body]
  `(def ~name (conduit-proc (fn ~name ~args ~@body))))

