(defproject net.intensivesystems/conduit "0.8.6"
  :description "Conduit: Stream Processing in Clojure."
  :dependencies [[org.clojure/clojure "1.2.1"]
                 [org.clojure/clojure-contrib "1.2.0"]
                 [net.intensivesystems/arrows "1.2.0"]]
  :dev-dependencies [[clj-stacktrace "0.2.2"]
                     [lein-difftest "1.3.3"]
                     [lein-fail-fast "1.0.0"]
                     [lein-release "1.1.1"]
                     [swank-clojure "1.2.1"]]
  ;; for ci purposes
  :repositories {"snapshots" "http://localhost/archiva/repository/snapshots"
                 "releases" "http://localhost/archiva/repository/internal"})
