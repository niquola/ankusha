(defproject node_test "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0-alpha6"]
                 [com.cemerick/piggieback "0.1.3"]
                 [org.clojure/clojurescript "1.9.36" :scope "provided"]]
  :profiles {:dev {:dependencies [[com.cemerick/piggieback "0.2.1"]
                                  [org.clojure/tools.nrepl "0.2.12"]]
                   :plugins [[lein-ancient "0.6.10"]
                             [lein-cljsbuild "1.1.3"]]
              :repl-options {:nrepl-middleware [cemerick.piggieback/wrap-cljs-repl]}}}
  :clean-targets [[:cljsbuild :builds 0 :compiler :output-to] :target-path :compile-path]
  :cljsbuild {:builds [{:id "dev"
                        :source-paths ["src"]
                        :compiler {:main "ankusha.core"
                                   :output-dir "out"
                                   :output-to "index.js"
                                   :target :nodejs}}]})

