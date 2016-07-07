(ns ankusha.lifebit
  (:require [ankusha.state :as state]
            [ankusha.log :as log]
            [ankusha.config :as conf]
            [ankusha.pg.core :as pg]
            [clojure.java.shell :as sh]
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


(defn pg-master [gcfg lcfg system-user]
  (pg/master
   {:data-dir (pg-data-dir lcfg)
    :hba      (:glb/hba gcfg)
    :users    (into [system-user] (or (:glb/users gcfg) []))
    :config     (postgresql-conf gcfg lcfg)}))

(defn init-system-user []
  (let [system-user {:usr/name "system"
                     :usr/password (str (java.util.UUID/randomUUID)) 
                     :usr/superuser true}]
    (ax/dvar-set "user" system-user)
    system-user))

(defn ax-node-state! [lcfg]
  (ax/dmap-put "nodes" (:lcl/name lcfg) lcfg))

(defn master! [lcfg ttl]
  #_(log/debug "update master" (/ (or ttl 6000) 1000) "sec")
  (ax/dvar-set "master" lcfg (/ (or ttl 6000) 1000)))

(defn on-bootstrap [gcfg lcfg]
  (let [m-ttl (master-ttl gcfg)
        system-user (init-system-user)]
    (pg-master gcfg lcfg system-user)
    (ax-node-state! lcfg)
    (master! lcfg m-ttl)
    (journal/write {:state :master})))


(defn master-health [state gcfg lcfg]
  (let [checks (get-in gcfg [:glb/lifebit state])
        user (ax/dvar! "user")]
    (sql/with-connection (db-connection-spec user lcfg) 
      (fn [conn]
        (->> checks
             (reduce (fn [acc [k v]] (assoc acc k (run-check conn k v))) {})
             (merge {:ts (util/now)})
             (ax/dmap-put "nodes-health" (:lcl/name lcfg)))))))

(defn master-to-new [lcfg]
  (log/info "Other master. Switching to replica")
  ;; !!!!!!!!!!!! FIXIT user rewind
  (pg/pg_ctl (pg-data-dir lcfg) :stop "-m" "fast")
  (sh/sh "rm" "-rf" (pg-data-dir lcfg))
  (journal/write {:state :new}))

(defn on-master [gcfg lcfg]
  (let [ttl (master-ttl gcfg)]
    (master-health :master gcfg lcfg)
    (if-let [mcfg (ax/dvar! "master")]
      (if (= (:lcl/name lcfg) (:lcl/name mcfg))
        (master! lcfg ttl)
        (master-to-new lcfg))
      (ax/dlock-try "master-lock" (master! lcfg ttl)))))

(defn follow [gcfg mcfg lcfg]
  (log/info "Switching to another master" (:lcl/name mcfg))
  (pg/pg_ctl (pg-data-dir lcfg) :stop "-m" "fast")
  (sh/sh "rm" "-rf" (pg-data-dir lcfg))
  (journal/write {:state :new})

  (comment
    user rewind
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

    (journal/write {:state :replica :master (:lcl/name mcfg)})))

(defn promote-replica [gcfg lcfg]
  (log/info "Acquire [master-lock] & start promoting")
  (pg/promote (pg-data-dir lcfg) (cfg-to-port lcfg))
  (master! lcfg (master-ttl gcfg))
  (journal/write {:state :master}))

(defn on-replica [local-state gcfg lcfg]
  (master-health :replica gcfg lcfg)
  (if-let [mcfg (ax/dvar! "master")]
    (when-not (= (:master local-state) (:lcl/name mcfg))
      (follow gcfg mcfg lcfg))
    (ax/dlock-try "master-lock" (promote-replica gcfg lcfg))))



(defn on-new [gcfg lcfg]
  (when-let [pcfg (ax/dvar! "master")]
    (log/info "Found master " (:lcl/name pcfg))
    (ax/dlock-try
     "basebackup"
     (let [user (ax/dvar! "user")
           primary-conn {:user     (:usr/name user)
                         :password (:usr/password user)
                         :host     (cfg-to-host pcfg)
                         :port     (cfg-to-port pcfg)}]

       (when-not user (throw (Exception. "Could not find cluster user")))

       (pg/replica primary-conn
                   {:data-dir (pg-data-dir lcfg)
                    :hba (:glb/hba gcfg)
                    :timeout 3000
                    :config (postgresql-conf gcfg lcfg)})

       (ax-node-state! lcfg)
       (journal/write {:state :replica :master (:lcl/name pcfg)})))))

(defn failover [gcfg lcfg]
  (let [{state :state :as local-state} (journal/jreduce merge {})]
    (try
      (cond
        (= state :bootstrap)    (on-bootstrap gcfg lcfg) 
        (= state :new)          (on-new gcfg lcfg)
        (= state :replica)      (on-replica local-state gcfg lcfg)
        (= state :master)       (on-master gcfg lcfg)
        :else (log/warn "Unhandled state " state (journal/jmap identity)))
      (catch Exception e
        (log/error "Error whil lifebit in" (str "[" state "]") e)))))

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
         (log/warn "Could not read global or local config" gcfg lcfg))))))
