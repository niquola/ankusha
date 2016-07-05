(ns ankusha.health
  (:require [ankusha.state :as state :refer [with-node]]
            [clojure.tools.logging :as log]
            [ankusha.config :as conf]
            [ankusha.atomix.core :as ax]
            [ankusha.util :as util]
            [ankusha.config :as conf]
            [ankusha.atomix.core :as ax]
            [ankusha.pg.core :as pg]
            [ankusha.pg.query :as sql]
            [clojure.core.async :as a :refer [>! <! go-loop alt! chan close! timeout]]))

(def health-check-timeout (atom 3000))
(def health-checks (atom
                    {:simple-read "SELECT 1"
                     :snapshot "SELECT txid_current_snapshot()"
                     :transaction-id "SELECT txid_current()"}))


(defn health-checks [gcfg lcfg]
  (sql/with-connection gcfg lcfg
    (fn [conn]
      (->>
       (get-in gcfg [:glb/health :chk/master])
       (reduce
        (fn [acc [k v]]
          (->>
           (try
             {:status :ok
              :sql v
              :result (str (sql/query-value conn v))}
             (catch Exception e
               (log/error "Health check:" (str e))
               {:status :fail
                :sql v
                :error (str e)}))
           (assoc acc k ))) {})
       (merge {:ts (str (java.time.Instant/now))})
       (ax/dmap-put "nodes-health" (:lcl/name lcfg))))))

(defn stop []
  (util/stop-checker :health))

(defn start [gcfg lcfg]
  (log/info "Start health checks for " (:lcl/name lcfg))
  (util/start-checker
   :health
   #(get-in gcfg [:glb/health :tx/timeout])
   (fn []
     (let [gcfg (ax/dvar! "config")
           lcfg (conf/local)]
       (when (and gcfg lcfg)
         (health-checks gcfg lcfg))))))


(comment
  (start)
  (stop)

  (ax/dmap! "nodes-health")

  (require '[ankusha.config :as conf])


  (health-checks
   (conf/load-global "sample/config.edn")
   (conf/load-local "sample/node-1.edn"))


  (with-node "node-2" (start))
  (with-node "node-3" (start))

  (with-node "node-2" (stop))
  (with-node "node-3" (stop)))
