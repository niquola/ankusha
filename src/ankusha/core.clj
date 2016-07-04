(ns ankusha.core
  (:require [ankusha.pg.core :as pg]
            [clojure.java.shell :as sh]
            [ankusha.config :as config]
            [clojure.tools.logging :as log]
            [ankusha.health :as health]
            [ankusha.failover :as failover]
            [ankusha.web :as web]
            [ankusha.state :as state :refer [with-node]]
            [ankusha.atomix.core :as ax]
            [clojure.string :as str])
  (:gen-class))

(defn bootstrap-master [local-config-path global-config-path]
  (config/load-local local-config-path)
  (let [cfg   (config/local)
        g-cfg (config/load-global global-config-path)]
    (ax/start cfg)
    (ax/bootstrap)
    (ax/dvar-set "config" g-cfg)

    (let [result (pg/master g-cfg cfg)]
      (ax/dmap-put "nodes" (:cfg/name cfg) result)
      (ax/dvar-set "master" result))))


(defn parse-addrs [addrs]
  (map (fn [x]
         (let [a (str/split x #":")]
           {:host (get a 0) :port (Integer/parseInt (get a 1))}))
       (str/split addrs #",")))

(defn bootstrap-replica [local-config-path addr-str]
  (let [cfg   (config/load-local local-config-path)
        addrs (parse-addrs addr-str)]
    (ax/start cfg)
    (ax/join addrs)))

(defn stop []
  (pg/stop)
  (web/stop)
  (health/stop)
  (failover/stop)
  (ax/shutdown))

(defn start [local-config]
  ;; check exists
  (let [cfg (config/load-local local-config)]
    (ax/start cfg)
    (ax/bootstrap)
    (pg/start)
    (web/start)
    (health/start)
    (failover/start)))

(comment
  (defn bootstrap-master [cfg]
    (ax/start cfg)
    (ax/bootstrap)

    (let [result (pg/master cfg)]
      (ax/dmap-put "nodes" (:name cfg) result)
      (ax/dvar-set "master" result)
      (health/start)))

  (defn bootstrap-replica [cfg addrs]
    (future
      (ax/start cfg)
      (ax/join addrs)

      (when-let [master-cfg (ax/dvar! "master" )]
        (log/info "Found master:" master-cfg)
        (log/info "Master config loaded:" master-cfg)
        (pg/replica master-cfg cfg)
        (ax/dmap-put  "nodes" (:name cfg) cfg)
        (health/start))))

  (defn stop-node []
    (health/stop)
    (pg/stop)
    (ax/shutdown))

  (defn start-node []
    (pg/start)
    (ax/start (state/get-in [:pg]))
    (ax/bootstrap)
    (health/start))
  )


(defn -main [] )

(comment
  (web/start 8080)
  (web/stop)

  (do
    (sh/sh "rm" "-rf" "/tmp/wallogs")
    (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")
    (sh/sh "rm" "-rf" "/tmp/node-1")
    (sh/sh "rm" "-rf" "/tmp/node-2")
    (sh/sh "rm" "-rf" "/tmp/node-3"))

  (state/with-node "node-1" (bootstrap-master  "sample/node-1.edn" "sample/config.edn"))

  (state/with-node "node-2" (bootstrap-replica "sample/node-2.edn" "127.0.0.1:4444"))
  (state/with-node "node-2" (bootstrap-replica "sample/node-3.edn" "127.0.0.1:4444"))

  (ax/status)

  (ax/dvar! "config")

  (with-node "node-1"
    (ax/dvar! "config"))


  (state/with-node "node-1" (stop))
  (state/with-node "node-3" (stop))
  (state/with-node "node-3" (stop))

  (state/with-node "node-1" (start "sample/node-1.edn"))
  (state/with-node "node-3" (start "sample/node-2.edn"))
  (state/with-node "node-3" (start "sample/node-3.edn"))

  (ax/status)
  (ax/leader)
  (pg/pid)
  )

