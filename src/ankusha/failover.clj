(ns ankusha.failover
  (:require [ankusha.state :as state]
            [clojure.tools.logging :as log]
            [ankusha.atomix.core :as cluster]
            [ankusha.util :as util]))

(defn join-cluster [gcfg mcfg lcfg])

(defn master-failover [gcfg mcfg lcfg])

(defn replica-failover [gcfg mcfg lcfg])

(defn newbie-failover [gcfg mcfg lcfg]
  (when mcfg (join-cluster gcfg mcfg lcfg)))

(defn failover [gcfg mcfg lcfg]
  (let [state (:lcl/state lcfg)]
    (condp = state
      :master  (master-failover gcfg mcfg lcfg)
      :replica (replica-failover gcfg mcfg lcfg)
      :newbie  (newbie-failover gcfg mcfg lcfg))))

(defn stop []
  (util/stop-checker :failover))

(defn start [gcfg lcfg]
  (util/start-checker
   :failover
   (fn [] (get-in gcfg [:glb/failover :tx/timeout]))
   (fn [] (log/info "Failover check"))))

(comment
  )
