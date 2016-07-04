(ns ankusha.failover
  (:require [ankusha.state :as state]
            [clojure.tools.logging :as log]
            [ankusha.atomix.core :as cluster]
            [ankusha.util :as util]))

(def failover-timeout (atom 1000))

(defn stop []
  (util/stop-checker :failover))

(defn start []
  (util/start-checker
   :failover
   (fn [] @failover-timeout)
   (fn [] (log/info "Failover check"))))

(comment)
