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

(defn start-services [gcfg lcfg]
  (web/start lcfg)
  (health/start gcfg lcfg)
  (failover/start gcfg lcfg))

(defn bootstrap-master [local-config-path global-config-path]
  (config/load-local local-config-path)
  (let [lcfg   (config/local)
        gcfg (config/load-global global-config-path)]
    (ax/start lcfg)
    (ax/bootstrap)
    (ax/dvar-set "config" gcfg)
    (pg/master gcfg lcfg)
    (ax/dmap-put "nodes" (:lcl/name lcfg) lcfg)
    (ax/dvar-set "master" lcfg (* 2 (get-in gcfg [:glb/failover :tx/timeout])))
    (start-services gcfg lcfg)))


(defn parse-addrs [addrs]
  (map (fn [x]
         (let [a (str/split x #":")] {:host (get a 0) :port (Integer/parseInt (get a 1))}))
       (str/split addrs #",")))

(defn bootstrap-replica [local-config-path addr-str]
  (let [lcfg (config/load-local local-config-path)
        addrs (parse-addrs addr-str)]
    (ax/start lcfg)
    (ax/join addrs)
    (let [gcfg (ax/dvar! "config")
          pcfg (ax/dvar! "master")]
      (log/info "global config:" gcfg)
      (log/info "master config:" pcfg)
      (pg/replica gcfg pcfg lcfg)
      (ax/dmap-put "nodes" (:lcl/name lcfg) lcfg)
      (start-services gcfg lcfg))))

(defn stop []
  (let [lcfg (config/local)]
    (pg/stop lcfg)
    (web/stop)
    (health/stop)
    (failover/stop)
    (ax/shutdown)))

(defn start [local-config]
  ;; check exists
  (let [lcfg (config/load-local local-config)]
    (ax/start lcfg)
    (ax/bootstrap)
    (let [gcfg (ax/dvar! "config")]
      (pg/pg_ctl lcfg :start)
      (start-services gcfg lcfg))))

(defn -main [] )

(comment
  (do
    (sh/sh "rm" "-rf" "/tmp/wallogs")
    (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")
    (sh/sh "rm" "-rf" "/tmp/node-1")
    (sh/sh "rm" "-rf" "/tmp/node-2")
    (sh/sh "rm" "-rf" "/tmp/node-3"))

  (state/with-node "node-1" (bootstrap-master  "sample/node-1.edn" "sample/config.edn"))

  (state/with-node "node-2" (bootstrap-replica "sample/node-2.edn" "127.0.0.1:4444"))

  (state/with-node "node-3" (bootstrap-replica "sample/node-3.edn" "127.0.0.1:4444"))

  (config/load-global "sample/config.edn")

  (failover/stop)

  (state/with-node "node-3"
    (ax/shutdown)
    (pg/pg_ctl (config/local) :stop))

  (ax/status)

  (ax/dvar! "config")

  (state/with-node "node-1" (stop))
  (state/with-node "node-2" (start "sample/node-1.edn"))

  (state/with-node "node-2" (stop))
  (state/with-node "node-2" (start "sample/node-2.edn"))

  (state/with-node "node-3" (stop))
  (state/with-node "node-3" (start "sample/node-3.edn"))

  (state/with-node "node-1" (start "sample/node-1.edn"))
  (state/with-node "node-3" (start "sample/node-2.edn"))
  (state/with-node "node-3" (start "sample/node-3.edn"))

  (ax/status)
  (ax/shutdown)
  (ax/leader)
  (pg/pid)
  )

