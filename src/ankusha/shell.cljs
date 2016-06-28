(ns ankusha.shell
  (:require-macros [cljs.core.async.macros :as m :refer [go alt!]])
  (:require [cljs.nodejs :as node]
            [cljs.core.async :as async :refer [<!]]))

(def proc (node/require "child_process"))
(def fs (node/require "fs"))

(defn- opts [m]
  (reduce (fn [acc [k v]] (into acc [(name k) v])) [] m))

(defn mk-args [args]
  (clj->js
   (if (map? (first args))
     (into (opts (first args)) (rest args))
     args)))

(defn spawn [cmd & args]
  (let [chan (async/chan)
        args (mk-args args)]
    (println "EXECUTE:" cmd args)
    (go
      (let [p (.spawn proc cmd args)]
        (.pipe (.-stdout p ) (.-stdout js/process))
        (.pipe (.-stderr p) (.-stderr js/process))
        (.on p "exit" (fn [code]
                        (async/put! chan code)
                        (async/close! chan)))))
    chan))

(defn xspawn [cmd & args]
  (let [args (if (map? (first args))
               (into (opts (first args)) (rest args))
               args)]
    (println "SPAWN:" cmd args)
    (let [p (.spawn proc cmd (clj->js args))]
      (.pipe (.-stdout p ) (.-stdout js/process))
      (.pipe (.-stderr p) (.-stderr js/process))
      (.on p "exit" (fn [code] (println "Exit with " code)))
      p)))

(defn spit [fl content]
  (let [chan (async/chan)]
    (.writeFile fs fl content
                (fn [err]
                  (when err (.log js/console err))
                  (async/close! chan)))))

(defn spit [fl content]
  (let [chan (async/chan)]
    (.writeFile fs fl content
                (fn [err]
                  (when err (.log js/console err))
                  (async/close! chan)))
    chan))

(defn slurp [fl]
  (let [chan (async/chan)]
    (.readFile fs fl
                (fn [err data]
                  (if err
                    (.log js/console err)
                    (async/put! chan (.toString data)))
                  (async/close! chan)))
    chan))


(comment
  (go (println "Status" (async/<! (spawn "ls" "-lah" "/tmp"))))

  (spit "/tmp/ups.txt" "hello")
  (go (println (<! (slurp "/tmp/ups.txt"))))
  )
