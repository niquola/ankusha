(ns ankusha.core
  (:require [ankusha.pg.core :as pg]
            [clojure.java.shell :as sh]
            [ankusha.config :as config]
            [ankusha.log :as log]
            [ankusha.lifebit :as lifebit]
            [ankusha.web :as web]
            [ankusha.state :as state :refer [with-node]]
            [ankusha.atomix.core :as ax]
            [ankusha.journal :as journal]
            [clojure.string :as str])
  (:gen-class))

(defn bootstrap-master [lcfg opts]
  (let [gcfg (config/load-global (:global-config opts))]
    (ax/start lcfg)
    (ax/bootstrap)
    (ax/dvar-set "config" gcfg)
    (web/start lcfg)
    (lifebit/start 3000)))

(defn parse-addrs [addrs]
  (map (fn [x] (let [a (str/split x #":")] {:host (get a 0) :port (Integer/parseInt (get a 1))}))
       (str/split addrs #",")))

(defn bootstrap-replica [lcfg {addr-str :join}]
  (let [addrs (parse-addrs addr-str)]
    (ax/start lcfg)
    (ax/join addrs)
    (web/start lcfg)
    (lifebit/start  3000)))

(defn reload-global-config [global-config-path]
  (let [gcfg (config/load-global global-config-path)]
    (ax/dvar-set "config" gcfg)))

(defn stop []
  (let [lcfg (config/local)]
    (pg/pg_ctl (str (:lcl/data-dir lcfg) "/pg") :stop)
    (web/stop)
    (lifebit/stop)
    (ax/shutdown)))

(defn start [lcfg]
  (ax/start lcfg)
  (ax/bootstrap)
  (let [gcfg (ax/dvar! "config")]
    (pg/pg_ctl (str (:lcl/data-dir lcfg) "/pg") :start)

    (web/start lcfg)
    (lifebit/start 3000)

    (ax/dmap-put "nodes" (:lcl/name lcfg) lcfg)))

(defn boot [local-config-path opts]
  (let [lcfg (config/load-local local-config-path)]
    (journal/start lcfg)

    (when (empty? (journal/jreduce merge {}))
      (when-not (:state opts) (throw (Exception. "Expected state for node, but " opts)))
      (journal/write {:state (:state opts)}))

    (let [{state :state :as local-state} (journal/jreduce merge {})]
      (cond
        (= state :bootstrap) (bootstrap-master lcfg opts)
        (= state :new)       (bootstrap-replica lcfg opts)
        (= state :replica)   (start lcfg)
        (= state :master)    (start lcfg)
        :else (throw (Exception. (str "Unknown initial state " state)))))))

(defn deinit []
  (journal/close))

(defn -main [])

(comment
  

  (do
    (do
      (pg/pg_ctl "/tmp/node-1/pg" :stop "-m" "fast")
      (pg/pg_ctl "/tmp/node-2/pg" :stop "-m" "fast")
      (pg/pg_ctl "/tmp/node-3/pg" :stop "-m" "fast")
      (sh/sh "rm" "-rf" "/tmp/wallogs")
      (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")
      (sh/sh "rm" "-rf" "/tmp/node-1")
      (sh/sh "rm" "-rf" "/tmp/node-2")
      (sh/sh "rm" "-rf" "/tmp/node-3")
      )

    (state/with-node "node-1"
      (boot  "sample/node-1.edn" {:state :bootstrap :global-config "sample/config.edn"}))

    (state/with-node "node-2"
      (boot "sample/node-2.edn" {:state :new :join "127.0.0.1:4444"}))

    (state/with-node "node-3"
      (boot "sample/node-3.edn" {:state :new :join "127.0.0.1:4444"}))
    )

  (ax/dvar! "master")
  (ax/dvar-set "master" nil)

  (state/with-node "node-1" (lifebit/stop))
  (state/with-node "node-1" (lifebit/start 3000))


  (state/with-node "node-2" (lifebit/stop))
  (state/with-node "node-2" (lifebit/start 3000))

  (state/with-node "node-3" (lifebit/stop))
  (state/with-node "node-3" (lifebit/start 3000))

  )

