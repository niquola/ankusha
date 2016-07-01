(ns ankusha.core
  (:require [ankusha.pg-cluster :as pg]
            [clojure.java.shell :as sh]
            [clojure.tools.logging :as log]
            [ankusha.web :as web]
            [ankusha.consensus :as cluster])
  (:gen-class))


(defn bootstrap-master [cfg]
  (cluster/shutdown (:name cfg))

  (cluster/start cfg)
  (cluster/bootstrap (:name cfg))

  (let [result (pg/master cfg)]
    (cluster/dmap-put (:name cfg) "nodes" (:name cfg) result)
    (cluster/dvar-set (:name cfg) "master" (:name cfg))))

(defn bootstrap-replica [cfg addrs]
  (future
    (cluster/start cfg)
    (cluster/join (:name cfg) addrs)
    (when-let [master (cluster/dvar! (:name cfg) "master" )]
      (log/info "Found master:" master)
      (when-let [master-cfg (cluster/dmap-get (:name cfg) "nodes" master)]
        (log/info "Master config loaded:" master-cfg)
        (pg/replica master-cfg cfg)
        (log/info "Start postgres:" (:name cfg))
        (pg/start (:name cfg))
        (cluster/dmap-put (:name cfg) "nodes" (:name cfg) cfg)))))

(defn stop-node [nm]
  (pg/stop nm)
  (cluster/shutdown nm))

(defn clean-up []
  (sh/sh "rm" "-rf" "/tmp/wallogs")
  (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")
  (sh/sh "rm" "-rf" "/tmp/node-1")
  (sh/sh "rm" "-rf" "/tmp/node-2")
  (sh/sh "rm" "-rf" "/tmp/node-3"))

(defn -main [] )

(comment
  (web/start)
  (web/stop)

  (clean-up)

  (bootstrap-master
   {:atomix-port 4444
    :port 5434
    :name "node-1"
    :data-dir "/tmp/node-1"})

  (cluster/status "node-1")
  (cluster/dvar! "node-1" "master" )
  (cluster/leader "node-1")

  (bootstrap-replica
   {:atomix-port 4445
    :name "node-2"
    :port 5435
    :data-dir "/tmp/node-2"}
   [{:port 4444 :host "localhost"}])

  (bootstrap-replica
   {:atomix-port 4446
    :name "node-3"
    :port 5436
    :data-dir "/tmp/node-3"}
   [{:port 4444 :host "localhost"}
    {:port 4445 :host "localhost"}])

  )

