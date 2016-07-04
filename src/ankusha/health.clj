(ns ankusha.health
  (:require [ankusha.state :as state :refer [with-node]]
            [clojure.tools.logging :as log]
            [ankusha.util :as util]
            [ankusha.config :as conf]
            [ankusha.atomix.core :as ax]
            [ankusha.pg.core :as pg]
            [clojure.core.async :as a :refer [>! <! go-loop alt! chan close! timeout]]))

(def health-check-timeout (atom 3000))
(def health-checks (atom
                    {:simple-read "SELECT 1"
                     :snapshot "SELECT txid_current_snapshot()"
                     :transaction-id "SELECT txid_current()"}))

(defn stop []
  (util/stop-checker :health))

(defn start [gcfg lcfg]
  (util/start-checker
   :health
   (fn [] (get-in gcfg [:glb/health :tx/timeout]))
   (fn []
     (let [res (reduce (fn [acc [k v]] (assoc acc k (pg/psql lcfg v))) {}
                       (get-in gcfg [:glb/health :chk/master]))]
       (ax/dmap-put "nodes-health"
                    (:lcl/name lcfg)
                    (assoc res :ts (str (java.time.Instant/now))))))))


(comment
  (start)
  (state/current)
  (stop)

  (with-node "node-2" (start))
  (with-node "node-3" (start))

  (with-node "node-2" (stop))
  (with-node "node-3" (stop)))
