(ns ankusha.pg
  (:require-macros [cljs.core.async.macros :as m :refer [go alt!]])
  (:require [cljs.core.async :as async]
            [clojure.walk :as walk]
            [honeysql.core :as hsql]
            [cljs.nodejs :as node]))

(node/enable-util-print!)

(def pg (node/require "pg"))
(def Client (.-Client pg))

(def conn "postgres://nicola:nicola@localhost:5432/postgres")

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
        cl (Client. conn)]
    (.connect
     cl
     (fn [err]
       (when err (async/put! ch {:error err}) (.log js/console "Error" err))
       (if (not err)
         (.query cl sql
                 (fn [err res]
                   (if err (.error js/console err)
                       (async/put! ch (.-rows res)))
                   (.end cl)))
         (do (async/put! ch {:error err}) (.log js/console "Error" err)))))
    ch))

(hsql/format (honey-macro {:select [[:$raw "1::text"]]}))

(defn exec [sql]
  (let [sql (if (map? sql) (hsql/format (honey-macro sql) :parameterizer :postgresql) [sql])
        _ (println "SQL:" sql)
        ch (async/chan)
        cl (Client. conn)]
    (println "Connect on " conn)
    (println "SQL:" sql)
    (.connect cl (fn [err]
       (if err (.log js/console "Error" err)
           (.query cl (first sql) (clj->js (rest sql))
                   (fn [err res]
                     (if err
                       (.error js/console "Error" err)
                       (async/put! ch (.-rows res)))
                     (.end cl))))))
    ch))

(comment
  (go
    (let [res (<! (exec {:select [1]}))]
      (.log js/console res)))
  )
