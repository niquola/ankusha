(ns ankusha.pg
  (:require-macros [cljs.core.async.macros :as m :refer [go alt!]])
  (:require [cljs.core.async :as async]
            [clojure.walk :as walk]
            [honeysql.core :as hsql]
            [honeysql.helpers :as h]
            [cljs.nodejs :as node]))

(node/enable-util-print!)

(def Client (node/require "pg-native"))

#_(def Client (.-Client pg))

(def hsql-macros
  {:$call hsql/call
   :$raw hsql/raw})

(defn honey-macro [hsql]
  (walk/postwalk
   (fn [x]
     (if-let [macro (and (vector? x) (get hsql-macros (first x)))]
       (apply macro (rest x))
       x))
   hsql))

(defn raw-exec [conn sql]
  (println "Try to connect" conn)
  (let [ch (async/chan)
        sql (if (vector? sql) sql [sql])
        cl (Client.)]
    (.connect
     cl conn
     (fn [err]
       (when err (async/put! ch {:error err}) (.log js/console "Error" err))
       (if (not err)
         (.query cl (first sql) (clj->js (rest sql))
                 (fn [err res]
                   (if err (.error js/console "QUERY ERROR:" err)
                       (async/put! ch (js->clj res)))
                   (.end cl)))
         (do (async/put! ch {:error err})
             (.log js/console "CONNECTION ERROR:" err)))))
    ch))

(hsql/format (honey-macro {:select [[:$raw "1::text"]]}))

(defn exec [conn sql]
  (raw-exec conn (if (map? sql)
                   (hsql/format (honey-macro sql) :parameterizer :postgresql)
                   [sql])))

(comment

  (let [conn "postgres:///postgres?host=/tmp/cluster-1&port=5434"]
    (go (println (async/<! (exec conn {:select [1]})))))

  (let [conn "postgres:///postgres?host=/tmp/cluster-1&port=5434"]
    (go (println (async/<! (raw-exec conn ["SELECT 1"])))))

  )
