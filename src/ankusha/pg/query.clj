(ns ankusha.pg.query
  (:require [clojure.java.jdbc :as sql]
            [clojure.tools.logging :as log])
  (:import (java.sql Connection DriverManager)))


(defn connection-string [host port]
  (str "jdbc:postgresql://" host ":" port "/postgres"))

(defn connection
  [{{{pswd :usr/password} :usr/replication} :glb/users}
   {host :lcl/host {port :pg/port} :lcl/postgres}]
  (Class/forName "org.postgresql.Driver")
  (DriverManager/getConnection (connection-string host port)
                               "replication" pswd))


(defn with-connection [gcfg lcfg f]
  (let [conn (connection gcfg lcfg)]
    (try
      (f conn)
      (finally (.close conn)))))

(defn query [conn sql]
  (sql/query {:connection conn} sql))

(defn query-first [conn sql]
  (first (query conn sql)))

(defn query-value [conn sql]
  (first (vals (query-first conn sql))))

(comment
  (def test-conn
    (connection
     {:glb/users {:usr/replication {:usr/password "replication"}}}
     {:lcl/host "localhost"
      :lcl/postgres {:pg/port 5432}}))

  (query-first test-conn "Select 1")

  (with-connection
   {:glb/users {:usr/replication {:usr/password "replication"}}}
   {:lcl/host "localhost"
    :lcl/postgres {:pg/port 5432}}
    (fn [conn]
      (query-value test-conn "select 1")))


  (try
    (query-first test-conn "Selec 1")
    (catch Exception e
      (str e)))

  )
