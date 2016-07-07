(ns ankusha.pg.query
  (:require [clojure.java.jdbc :as sql]
            [ankusha.log :as log])
  (:import (java.sql Connection DriverManager)))


(defn connection-string [host port]
  (str "jdbc:postgresql://" host ":" port "/postgres"))

(defn connection
  [{host :host port :port user :user pswd :password }]
  (Class/forName "org.postgresql.Driver")
  (DriverManager/getConnection (connection-string host port)
                               user pswd))

(defn with-connection [spec f]
  (let [conn (connection spec)]
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

  (with-connection
    {:user "replication"
     :password "secret"
     :host "localhost"
     :port 5432}
    (fn [conn]
      (query-value conn "select 1"))) 
  )
