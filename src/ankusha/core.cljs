(ns ankusha.core
  (:require-macros [cljs.core.async.macros :as m :refer [go alt!]])
  (:require [cljs.nodejs :as node]
            [cljs.core.async :as async]
            [ankusha.consul :as consul]
            [ankusha.pg :as pg]))

(node/enable-util-print!)

(def proc (node/require "child_process"))

(defn exec [cmd & args]
  (let [p (.spawn proc cmd (clj->js args))]
    (.pipe (.-stdout p ) (.-stdout js/process))
    (.pipe (.-stderr p) (.-stderr js/process))
    (.on p "exit" (fn [code] (println "Exit code:" code)))))

(defn -main []
  (.log js/console "Hello all")
  (exec "ls" "-lah")
  (go
    (let [res (<! (pg/exec {:select [1]}))]
      (.log js/console res)))
  (consul/start))

(set! *main-cli-fn* -main)

(def pg-config
  {:shared-buffers "100m"})

(defn generate-config [cfg]
  (with-out-str
    (println "shared-buffers " (:shared-buffers cfg))))

(comment
  (-main)
  )
