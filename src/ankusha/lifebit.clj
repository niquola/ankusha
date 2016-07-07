(ns ankusha.lifebit
  (:require [ankusha.state :as state]
            [ankusha.log :as log]
            [ankusha.config :as conf]
            [ankusha.pg.core :as pg]
            [ankusha.journal :as journal]
            [clojure.string :as str]
            [ankusha.pg.query :as sql]
            [ankusha.atomix.core :as ax]
            [ankusha.util :as util]))

(defn master-ttl [gcfg]
  (* 2 (get-in gcfg [:glb/lifebit :tx/timeout])))

(defn pg-data-dir [cfg & args]
  (apply str (:lcl/data-dir cfg) "/pg" (when-not (empty? args) (str "/" (str/join "/" args)))))

(defn postgresql-conf [gcfg lcfg]
  (-> (merge (:glb/postgres gcfg)
             (:lcl/postgres lcfg)
             {:pg/unix_socket_directories (pg-data-dir lcfg) 
              :pg/log_line_prefix (str "[" (:lcl/name lcfg) "]")})
      (dissoc
       :pg/restore_command
       :pg/archive_cleanup_command)))

(defn run-check [conn k v]
  (try
    {:status :ok
     :sql v
     :result (str (sql/query-value conn v))}
    (catch Exception e
      {:status :fail
       :sql v
       :error (str e)})))

(defn cfg-to-port [cfg]
  (get-in cfg [:lcl/postgres :pg/port]))

(defn cfg-to-host [cfg]
  (:lcl/host cfg))

(defn db-connection-spec [user lcfg]
  (let [{user :usr/name pswd :usr/password} user
        {host :lcl/host {port :pg/port} :lcl/postgres} lcfg]
    {:host host :port port :user user :password pswd}))

(defn postgresql-conf [gcfg lcfg]
  (-> (merge (:glb/postgres gcfg)
             (:lcl/postgres lcfg)
             {:pg/unix_socket_directories (pg-data-dir lcfg) 
              :pg/log_line_prefix (str "[" (:lcl/name lcfg) "]")})
      (dissoc
       :pg/restore_command
       :pg/archive_cleanup_command)))

(defn pg-master [gcfg lcfg system-user]
  (pg/master
   {:data-dir (pg-data-dir lcfg)
    :hba      (:glb/hba gcfg)
    :users    (into [system-user] (or (:glb/users gcfg) []))
    :config     (postgresql-conf gcfg lcfg)}))

(defn bootstrap-failover [gcfg lcfg]
  (try
    (let [system-user {:usr/name "system"
                       :usr/password (str (java.util.UUID/randomUUID)) 
                       :usr/superuser true}]
      (journal/write {:state :initdb})
      (pg-master gcfg lcfg system-user)
      (ax/dvar-set "user" system-user)

      (ax/dmap-put "nodes" (:lcl/name lcfg) lcfg)
      (ax/dvar-set "master" lcfg (* 2 (get-in gcfg [:glb/lifebit :tx/timeout])))
      (journal/write {:state :master}))
    (catch Exception e
      (log/error "while master failover" (pr-str e)))))


(defn master-health [state gcfg lcfg]
  (let [checks (get-in gcfg [:glb/lifebit state])
        user (ax/dvar! "user")]
    (sql/with-connection (db-connection-spec user lcfg) 
      (fn [conn]
        (->> checks
             (reduce (fn [acc [k v]] (assoc acc k (run-check conn k v))) {})
             (merge {:ts (util/now)})
             (ax/dmap-put "nodes-health" (:lcl/name lcfg)))))))

(defn master-failover [gcfg lcfg]
  (try
    (let [ttl (master-ttl gcfg)]
      (master-health :master gcfg lcfg)
      (if-let [mcfg (ax/dvar! "master")]
        (if (= (:lcl/name lcfg) (:lcl/name mcfg))
          (do #_(log/debug "Renew master record")
              (ax/dvar-set "master" lcfg ttl))
          (do (log/info "Other master. Switching to replica")
              (journal/write {:state :swithcing})
              (pg/switch gcfg mcfg lcfg)
              (journal/write {:state :replica :master (:lcl/name mcfg)})))
        (if (ax/dlock-try "master-lock")
          (do (log/info "Acquire [master-lock] & renew master record")
              (ax/dvar-set "master" lcfg ttl)
              (ax/dlock-release "master-lock"))
          (log/info "Unable to acquire [master-lock]"))))
    (catch Exception e
      (log/error "while master failover" (pr-str e)))))

(defn replica-failover [local-state gcfg lcfg]
  (try
    (master-health :replica gcfg lcfg)
    (if-let [mcfg (ax/dvar! "master")]
      (when-not (= (:master local-state) (:lcl/name mcfg))
        (log/info "Switching to another master")
        (journal/write {:state :swithcing})

        (let [user (ax/dvar! "user")
              primary-conn {:user     (:usr/name user)
                            :password (:usr/password user)
                            :host     (cfg-to-host mcfg)
                            :port     (cfg-to-port mcfg)}]
          (when-not user (throw (Exception. "Could not find cluster user")))
          (pg/switch primary-conn
                     {:data-dir (pg-data-dir lcfg)
                      :timeout 3000
                      :config (postgresql-conf gcfg lcfg)}))

        (journal/write {:state :replica :master (:lcl/name mcfg)}))
      (if (ax/dlock-try "master-lock")
        (do (log/info "Acquire [master-lock] & start promoting")
            (journal/write {:state :promote-replica})

            (pg/promote (pg-data-dir lcfg) (cfg-to-port lcfg))

            (ax/dvar-set "master" lcfg (master-ttl gcfg))
            (journal/write {:state :master})
            (ax/dlock-release "master-lock"))
        (log/info "Unable to acquire [master-lock]")))
    (catch Exception e
      (log/error "while replica failover" (pr-str e)))))

(defn new-failover [gcfg lcfg]
  (log/info "Start replica initialization" lcfg)
  (when-let [pcfg (ax/dvar! "master")]
    (journal/write {:state :init-replica})
    (log/info "global config:" gcfg)
    (log/info "master config:" pcfg)


    (let [user (ax/dvar! "user")
          primary-conn {:user     (:usr/name user)
                        :password (:usr/password user)
                        :host (cfg-to-host pcfg)
                        :port  (cfg-to-port pcfg)}]
      (when-not user (throw (Exception. "Could not find cluster user")))
      (pg/replica primary-conn
                 {:data-dir (pg-data-dir lcfg)
                  :hba (:glb/hba gcfg)
                  :timeout 3000
                  :config (postgresql-conf gcfg lcfg)}))


    (ax/dmap-put "nodes" (:lcl/name lcfg) lcfg) 
    (journal/write {:state :replica :master (:lcl/name pcfg)})))


(defn failover [gcfg lcfg]
  (let [local-state (journal/jreduce merge {})
        state (:state local-state)]
    (cond
      (= state :bootstrap)    (bootstrap-failover gcfg lcfg) 
      (= state :new)          (new-failover gcfg lcfg)
      (= state :init-replica) (log/info "Initing replica...")
      (= state :replica)      (replica-failover local-state gcfg lcfg)
      (= state :promote-replica) (log/info "Promoting to master")
      (= state :master)       (master-failover gcfg lcfg)
      :else (log/error (str "Unknown initial state " state)
                       lcfg (journal/jreduce merge {})))))

(defn stop []
  (util/stop-checker :failover))

(defn start [start-timeout]
  (stop)
  (log/info "Start lifebit")
  (util/start-checker
   :failover
   (constantly start-timeout)
   (fn []
     (let [gcfg (ax/dvar! "config")
           lcfg (conf/local)]
       (if (and gcfg lcfg)
         (failover gcfg lcfg)
         (log/warn "Could not read global or local config"))))))
