(ns ankusha.pg-cluster
  (:require [clojure.string :as str]
            [ankusha.config :as pg-config :refer [pg-data-dir]]
            [ankusha.state :as state]
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
      (do
        (log/error res)
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

(defn get-node []
  (state/get-in [:pg] ))

(defn pg_ctl [cfg cmd]
  (log/info "[" (:name cfg) "]" "pg_ctl" cmd)
  (sh/sh (bin "pg_ctl") "-D" (pg-data-dir cfg) "-l" (pg-data-dir cfg "/postgres.log") (name cmd)))

(defn psql [cfg sql]
  (let [res (sh/sh "psql"
                   "postgres"
                   "-h" (pg-data-dir cfg)
                   "-p" (str (:port cfg))
                   "-c" sql)]
    (log/info "psql:" res)
    res))

(defn sql [sql]
  (when-let [cfg (state/get-in [:pg])]
    (psql cfg sql)))

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
  (log/info "[" (:name cfg) "] " "initdb -D" (pg-data-dir cfg))
  (let [res (sh/sh (bin "initdb") "-D" (pg-data-dir cfg))]
    (if (= 0 (:exit res))
      (log/info (:out res))
      (throw (Exception. (str res))))
    (when-let [err (:err res)] (log/warn err))))

(defn pid []
  (let [cfg (get-node)
        pth (pg-data-dir cfg "/postmaster.pid")]
    (when (.exists (io/file pth))
      (first (str/split (slurp pth) #"\n")))))

(defn start []
  (if-let [cfg (get-node)]
    (if (.exists (io/file (pg-data-dir cfg "/postmaster.pid")))
      (log/info "Postgres already started")
      (let [res (pg_ctl cfg :start)]
        (state/assoc-in [:pg :process] (pid))
        res))
    (log/info "Node not initialized")))

(defn stop []
  (pg_ctl (get-node) :stop))


(defn create-user [cfg {nm :name pswd :password}]
  (log/info "Create user" nm )
  (psql cfg (str  "CREATE USER " nm " WITH SUPERUSER PASSWORD '" pswd "'")))

(defn master [opts]
  (let [cfg (merge (node-config opts)
                   {:user {:name "ankus" :password (str (java.util.UUID/randomUUID))}})]
    (initdb cfg)
    (pg-config/update-config cfg)
    (pg-config/update-hba cfg)
    (state/assoc-in [:pg] cfg)
    (start)
    (wait-pg cfg 10 "SELECT 1")
    (create-user cfg (:user cfg))
    cfg))

(defn kill [pid sig]
  (sh/sh "kill" (str "-" (str/upper-case (name sig))) pid))

(defn replica [parent-cfg replica-opts]
  (let [cfg (node-config replica-opts)
        pgpass-path (str (:data-dir replica-opts) "/.pgpass")
        args [(bin "pg_basebackup")
              "-w"
              "-h" (:host parent-cfg)
              "-p" (str (:port parent-cfg))
              "-U" (get-in parent-cfg [:user :name])
              "-c" "fast"
              "-D" (pg-data-dir cfg)
              :env {"PGPASSFILE" pgpass-path}]]

    (sh! "mkdir" "-p" (:data-dir cfg))
    (sh! "chmod" "0700" (:data-dir cfg))
    (spit pgpass-path
          (let [{{u :name pwd :password} :user h :host p :port} parent-cfg]
            (str h ":" p ":*:" u ":" pwd "\n")))
    (log/info "chmod .pgpass")
    (sh! "chmod" "0600" pgpass-path)
    (log/info args)
    (let [res (apply sh/sh args)]
      (if (= 0 (:exit res))
        (log/info "basebackup done")
        (throw (Exception. (str "Basebackup failed: " res)))))
    (sh! "rm" "-f" pgpass-path)
    (pg-config/update-config cfg)
    (pg-config/update-hba cfg)
    (pg-config/update-recovery parent-cfg cfg)
    (state/assoc-in [:pg] cfg)
    (let [res (start)]
      (when-not (= 0 (:exit res))
        (throw (Exception. (str res)))))
    (wait-pg cfg 600)))

(defn clean-up []
  (sh/sh "rm" "-rf" "/tmp/wallogs")
  (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")
  (sh/sh "rm" "-rf" "/tmp/node-1")
  (sh/sh "rm" "-rf" "/tmp/node-2")
  (sh/sh "rm" "-rf" "/tmp/node-3"))


(comment
  (sh/sh "rm" "-rf" "/tmp/wallogs")
  (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")

  (clean-up)

  (for [x (enumeration-seq (java.net.NetworkInterface/getNetworkInterfaces))]
    x)

  (def cfg
    {:name "node-1"
     :host "127.0.0.1"
     :port 5434
     :data-dir "/tmp/node-1"})

  (def master-cfg (master cfg))

  (:user master-cfg)
  (:host master-cfg)

  (start)
  (stop)

  (sh! "rm" "-rf" "/tmp/node-2")

  (state/with-node "node-2"
    (replica master-cfg
             {:name "node-2"
              :host "127.0.0.1"
              :port 5435
              :data-dir "/tmp/node-2"}))

  (start)
  (state/with-node "node-2" (stop))


  (state/with-node "node-3"
    (replica master-cfg 
             {:name "node-3"
              :host "127.0.0.1"
              :port 5436
              :data-dir "/tmp/node-3"}))

  (state/with-node "node-3" (stop))
  )
