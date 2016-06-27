(ns ankusha.core
  (:require [cljs.nodejs :as node]))

(node/enable-util-print!)

(def proc (node/require "child_process"))


(defn exec [cmd & args]
  (let [p (.spawn proc cmd (clj->js args))]
    (.pipe (.-stdout p ) (.-stdout js/process))
    (.pipe (.-stderr p) (.-stderr js/process))
    (.on p "exit" (fn [code] (println "Exit code:" code)))))

(defn -main []
  (.log js/console "Hello all")
  (exec "ls" "-lah"))

(set! *main-cli-fn* -main)

(def pg-config
  {:shared-buffers "100m"})

(defn generate-config [cfg]
  (with-out-str
    (println "shared-buffers " (:shared-buffers cfg))))

(comment
  (generate-config pg-config))
