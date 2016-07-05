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
            [ankusha.journal :as journal]
            [clojure.string :as str])
  (:gen-class))

(defn start-services [gcfg lcfg]
  (web/start lcfg)
  (health/start gcfg lcfg)
  (failover/start gcfg lcfg))

(defn bootstrap-master [lcfg opts]
  (let [gcfg (config/load-global (:global-config opts))]
    (ax/start lcfg)
    (ax/bootstrap)

    (ax/dvar-set "config" gcfg)
    (pg/master gcfg lcfg)
    (ax/dmap-put "nodes" (:lcl/name lcfg) lcfg)
    (ax/dvar-set "master" lcfg (* 2 (get-in gcfg [:glb/failover :tx/timeout])))
    (start-services gcfg lcfg)

    (journal/write {:state :master :ts (java.util.Date.)})
    (state/assoc-in [:local-state] :master)))


(defn parse-addrs [addrs]
  (map (fn [x]
         (let [a (str/split x #":")] {:host (get a 0) :port (Integer/parseInt (get a 1))}))
       (str/split addrs #",")))

(defn bootstrap-replica [lcfg {addr-str :join}]
  (let [addrs (parse-addrs addr-str)]
    (ax/start lcfg)
    (ax/join addrs)

    (comment "Should be in cycle")
    (let [gcfg (ax/dvar! "config")
          pcfg (ax/dvar! "master")]
      (log/info "global config:" gcfg)
      (log/info "master config:" pcfg)
      (pg/replica gcfg pcfg lcfg)
      (ax/dmap-put "nodes" (:lcl/name lcfg) lcfg)
      (start-services gcfg lcfg)

      (journal/write {:state :replica :ts (java.util.Date.)})
      (state/assoc-in [:local-state] :replica))))

(defn reload-global-config [global-config-path]
  (let [gcfg (config/load-global global-config-path)]
    (ax/dvar-set "config" gcfg)))

(defn stop []
  (let [lcfg (config/local)]
    (pg/stop lcfg)
    (web/stop)
    (health/stop)
    (failover/stop)
    (ax/shutdown)))

(defn start [lcfg]
  (ax/start lcfg)
  (ax/bootstrap)
  (let [gcfg (ax/dvar! "config")]
    (pg/pg_ctl lcfg :start)
    (start-services gcfg lcfg)
    (ax/dmap-put "nodes" (:lcl/name lcfg) lcfg)))

(defn boot [local-config-path opts]
  (let [lcfg (config/load-local local-config-path)]
    (journal/start lcfg)

    (when (empty? (journal/jreduce merge {}))
      (when-not (:state opts) (throw (Exception. "Expected state for node, but " opts)))
      (log/info {:state (:state opts) :ts (java.util.Date.)})
      (journal/write {:state (:state opts) :ts (java.util.Date.)}))

    (let [{state :state :as local-state} (journal/jreduce merge {})]
      (state/assoc-in [:local-state] local-state)
      (cond
        (= state :bootstrap) (bootstrap-master lcfg opts)
        (= state :new)      (bootstrap-replica lcfg opts)
        (= state :replica)  (start lcfg)
        (= state :master)   (start lcfg)
        :else (throw (Exception. (str "Unknown initial state " state)))))))

(defn deinit []
  (journal/close))

(defn -main [])

(comment
  (do
    (sh/sh "rm" "-rf" "/tmp/wallogs")
    (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")
    (sh/sh "rm" "-rf" "/tmp/node-1")
    (sh/sh "rm" "-rf" "/tmp/node-2")
    (sh/sh "rm" "-rf" "/tmp/node-3"))

  (state/with-node "node-1"
    (future
      (boot  "sample/node-1.edn" {:state :bootstrap :global-config "sample/config.edn"})))

  (journal/jmap identity)

  (state/with-node "node-2"
    (future
      (boot "sample/node-2.edn" {:state :new :join "127.0.0.1:4444"})))

  (state/with-node "node-3"
    (future
      (boot "sample/node-3.edn" {:state :new :join "127.0.0.1:4444"})))

  (reload-global-config "sample/config.edn")

  (state/with-node "node-1" (stop))
  (state/with-node "node-2" (stop))
  (state/with-node "node-3" (stop))
  
  (ax/status)
  (ax/shutdown)
  (ax/leader)
  (pg/pid)
  )

