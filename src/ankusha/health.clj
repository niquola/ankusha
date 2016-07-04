(ns ankusha.health
  (:require [ankusha.state :as state]
            [clojure.tools.logging :as log]
            [ankusha.consensus :as cluster]
            [ankusha.pg-cluster :as pg]
            [clojure.core.async :as a :refer [>! <! go-loop alt! chan close! timeout]]))

(def health-checks (atom
                    {:simple-read "SELECT 1"
                     :snapshot "SELECT txid_current_snapshot()"
                     :transaction-id "SELECT txid_current()"}))

(defn stop-health-check []
  (when-let [hc (state/get-in [:health-check])]
    (close! hc)))

(defn start-health-check []
  (stop-health-check)
  (let [stop (chan)]
    (go-loop []
      (let [tm (timeout 1000)]
        (alt!
          tm (do
               (let [res (reduce (fn [acc [k v]] (assoc acc k (pg/sql v))) {} @health-checks)]
                 (cluster/dmap-put "nodes-health" (state/current) (assoc res :ts (str (java.util.Date.))))
                 (recur)))
          stop (log/info "Stop channel"))))
    (state/assoc-in [:health-check] stop)))


(comment
  (start-health-check)
  (state/current)
  (stop-health-check)

  (state/with-node "node-2"
    (start-health-check))

  (state/with-node "node-3"
    (start-health-check))

  (state/with-node "node-2"
    (stop-health-check))

  (state/with-node "node-3"
    (stop-health-check))

  (pg/sql "SELECT 1"))
