(ns ankusha.core
  (:require [ankusha.pg-cluster :as pg]
            [clojure.java.shell :as sh]
            [clojure.tools.logging :as log]
            [ankusha.web :as web]
            [ankusha.state :as state]
            [ankusha.consensus :as cluster])
  (:gen-class))


(defn bootstrap-master [cfg]

  (cluster/start cfg)
  (cluster/bootstrap)

  (let [result (pg/master cfg)]
    (cluster/dmap-put "nodes" (:name cfg) cfg)
    (cluster/dvar-set "master" (:name cfg))))

(defn bootstrap-replica [cfg addrs]
  (future
    (cluster/start cfg)
    (cluster/join addrs)
    (when-let [master (cluster/dvar! "master" )]
      (log/info "Found master:" master)
      (when-let [master-cfg (cluster/dmap-get "nodes" master)]
        (log/info "Master config loaded:" master-cfg)
        (pg/replica master-cfg cfg)
        (log/info "Start postgres:" (:name cfg))
        (pg/start)
        (cluster/dmap-put  "nodes" (:name cfg) cfg)))))

(defn stop-node []
  (pg/stop)
  (cluster/shutdown))

(defn clean-up []
  (sh/sh "rm" "-rf" "/tmp/wallogs")
  (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")
  (sh/sh "rm" "-rf" "/tmp/node-1")
  (sh/sh "rm" "-rf" "/tmp/node-2")
  (sh/sh "rm" "-rf" "/tmp/node-3"))

(defn -main [] )

(comment
  (web/start 8080)
  (web/stop)

  (clean-up)


  (state/with-node "node-1"
    (bootstrap-master
     {:atomix-port 4444
      :port 5434
      :name "node-1"
      :data-dir "/tmp/node-1"}))

  (future
    (println (cluster/dmap-get "nodes" (cluster/dvar! "master"))))

  (cluster/status)
  (cluster/leader)
  (pg/pid)

  (future (stop-node))

  (state/with-node "node-3"
    (future (stop-node)))

  (state/with-node "node-2"
    (future (stop-node)))

  (state/with-node "node-2"
    (bootstrap-replica
     {:atomix-port 4445
      :name "node-2"
      :port 5435
      :data-dir "/tmp/node-2"}
     [{:port 4444 :host "localhost"}]))

  (state/with-node "node-2"
    (cluster/shutdown)
    (pg/stop))

  (state/with-node "node-3"
    (bootstrap-replica
     {:atomix-port 4446
      :name "node-3"
      :port 5436
      :data-dir "/tmp/node-3"}
     [{:port 4444 :host "localhost"}
      {:port 4445 :host "localhost"}]))

  (cluster/dmap! "nodes")

  (state/with-node "node-1"
    (pg/stop)
    (future (stop-node)))

  (state/with-node "node-2"
    (pg/stop)
    (future (stop-node)))

  (state/with-node "node-3"
    (pg/stop)
    (future (stop-node)))

  )

