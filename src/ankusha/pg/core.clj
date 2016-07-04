(ns ankusha.pg.core
  (:require [clojure.string :as str]
            [ankusha.pg.config :as pg-config :refer [pg-data-dir]]
            [ankusha.state :as state :refer [with-node]]
            [clojure.java.shell :as sh]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go alt! go-loop <!]]
            [clojure.java.io :as io]))

(defonce config (atom {:bin "/usr/lib/postgresql/9.5/bin"}))

(defn bin [util] (str (:bin @config) "/" util))

(defn sh! [& args]
  (log/info args)
  (let [res (apply sh/sh args)]
    (if (= 0 (:exit res))
      res
      (do (log/error res)
        (throw (Exception. (str res)))))))

(defn node-config [opts]
  (assert (:data-dir opts))
  (let [data-dir (:data-dir opts)]
    (merge opts {:hba [[:local :all :all :trust]
                       [:local :replication :ankus :trust]
                       [:host  :replication :ankus "0.0.0.0/0" :md5]
                       [:host :all :all "0.0.0.0/0" :md5]]
                 :config {:max_connections 100
                          :listen_addresses "*"
                          :unix_socket_directories (pg-data-dir opts) 

                          :wal_level "logical"
                          :hot_standby "on"
                          :archive_mode "on"
                          :archive_command "cp %p /tmp/wallogs/%p" 
                          :archive_timeout 60
                          :max_wal_senders 2
                          :wal_keep_segments 100

                          :shared_buffers  "128MB"
                          :log_line_prefix  (str (:name opts) ": ")

                          :port (:port opts)}})))

(defn pg_ctl [lcfg cmd]
  (sh! (bin "pg_ctl") "-D" (pg-data-dir lcfg) "-l" (pg-data-dir lcfg "/postgres.log") (name cmd)))

(defn psql [lcfg sql]
  (let [res (sh/sh "psql"
                   "postgres"
                   "-h" (pg-data-dir lcfg)
                   "-p" (str (get-in lcfg [:lcl/postgres :pg/port]))
                   "-c" sql)]
    (log/info "psql:" res)
    res))

(defn psql! [lcfg sql]
  (sh! "psql"
       "postgres"
       "-h" (pg-data-dir lcfg)
       "-p" (str (get-in lcfg [:lcl/postgres :pg/port]))
       "-c" sql))

(defn wait-pg [cfg sec & [sql]]
  (loop [n sec]
    (if (> n 0)
      (let [res (psql cfg (or sql "SELECT 1"))]
        (log/info "Connect to pg:" res)
        (when (not (= 0 (:exit res)))
          (Thread/sleep 1000)
          (recur (dec n))))
      (throw (Exception. (str "Unable to connect to postgres"))))))

(defn initdb [cfg]
  (log/info "[" (:lcl/name cfg) "] " "initdb -D" (pg-data-dir cfg))
  (let [res (sh/sh (bin "initdb") "-D" (pg-data-dir cfg))]
    (if (= 0 (:exit res))
      (log/info (:out res))
      (throw (Exception. (str res))))
    (when-let [err (:err res)] (log/warn err))))

(defn pid [lcfg]
  (let [pth (pg-data-dir lcfg "/postmaster.pid")]
    (when (.exists (io/file pth))
      (first (str/split (slurp pth) #"\n")))))

(defn stop [lcfg]
  (pg_ctl lcfg :stop))

(defn create-user [lcfg users]
  (doseq [[nm usr] users]
    (psql lcfg (str  "CREATE USER " (name nm) " WITH SUPERUSER PASSWORD '" (:usr/password usr) "'"))))

(defn spit-config [lcfg filename content]
  (log/info "Write " filename "\n" content)
  (spit (pg-data-dir lcfg filename)
        content))


(defn update-config [gcfg lcfg]
  (spit-config lcfg "postgresql.conf"
               (pg-config/config gcfg lcfg))
  (spit-config lcfg "pg_hba.conf"
               (pg-config/hba gcfg lcfg)))

(defn master [gcfg lcfg]
  (initdb lcfg)
  (update-config gcfg lcfg)
  (pg_ctl lcfg :start)
  (wait-pg lcfg 10 "SELECT 1")
  (create-user lcfg (:glb/users gcfg)))

(defn kill [pid sig]
  (sh/sh "kill" (str "-" (str/upper-case (name sig))) pid))

(defn base-backup [gcfg pcfg lcfg]
  (let [pswd (get-in gcfg [:glb/users :usr/replication :usr/password])
        phost (:lcl/host pcfg)
        pport (get-in pcfg [:lcl/postgres :pg/port])
        pgpass-path (str (:lcl/data-dir lcfg)  "/.pgpass")
        pgpass-content (str phost ":" pport ":*:replication:" pswd "\n")]
    (spit pgpass-path pgpass-content)
    (sh! "chmod" "0600" pgpass-path)
    (sh! (bin "pg_basebackup")
         "-w"
         "-h" (:lcl/host pcfg)
         "-p" (str (get-in pcfg [:lcl/postgres :pg/port]))
         "-U" "replication"
         "-c" "fast"
         "-D" (pg-data-dir lcfg)
         :env {"PGPASSFILE" pgpass-path})
    (sh! "rm" "-f" pgpass-path)))

(defn replica [gcfg pcfg {data-dir :lcl/data-dir :as lcfg}]
  (sh! "mkdir" "-p" data-dir)
  (sh! "chmod" "0700" data-dir)
  (base-backup gcfg pcfg lcfg)
  (update-config gcfg lcfg)

  (spit-config
   lcfg "recovery.conf"
   (pg-config/recovery gcfg pcfg lcfg))

  (pg_ctl lcfg :start)

  (wait-pg lcfg (get-in gcfg [:glb/recovery :tx/timeout])))

(comment
  (do
    (sh/sh "rm" "-rf" "/tmp/wallogs")
    (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")
    (sh/sh "rm" "-rf" "/tmp/node-1")
    (sh/sh "rm" "-rf" "/tmp/node-2")
    (sh/sh "rm" "-rf" "/tmp/node-3"))

  (require '[ankusha.config :as conf])

  (conf/load-local "sample/node-1.edn")

  (conf/load-global "sample/config.edn")

  (sh/sh "rm" "-rf" "/tmp/node-1/pg")

  (master (conf/global) (conf/local))

  (pg_ctl (conf/local) :stop)

  (pg_ctl (conf/local) :start)

  (with-node "node-2" (pg_ctl (conf/local) :stop))

  (sh/sh "rm" "-rf" "/tmp/node-2")

  (with-node "node-2"
    (conf/load-global "sample/config.edn")
    (conf/load-local "sample/node-2.edn")
    (replica (conf/global)
             (with-node "node-1" (conf/local))
             (conf/local)))


  )
