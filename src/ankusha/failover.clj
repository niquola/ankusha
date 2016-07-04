(ns ankusha.failover
  (:require [ankusha.state :as state]
            [clojure.tools.logging :as log]
            [ankusha.atomix.core :as cluster]
            [ankusha.util :as util]))

(defn stop []
  (util/stop-checker :failover))

(defn start [gcfg lcfg]
  (util/start-checker
   :failover
   (fn [] (get-in gcfg [:glb/failover :tx/timeout]))
   (fn [] (log/info "Failover check"))))

(comment)
