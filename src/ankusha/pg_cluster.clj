(ns ankusha.pg-cluster
  (:require [clojure.string :as str]
            [ankusha.config :as pg-config :refer [pg-data-dir]]
            [ankusha.pg :as pg]
            [ankusha.state :as state]
            [clojure.java.shell :as sh]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [go alt! go-loop <!]]
            [clojure.java.io :as io]))

(defonce config (atom {:bin "/usr/lib/postgresql/9.5/bin"}))

(defn bin [util] (str (:bin @config) "/" util))

(defn node-config [opts]
  (assert (:data-dir opts))
  (let [data-dir (:data-dir opts)]
    (merge opts {:hba [[:local :all :all :trust]
                       [:local :replication :nicola :trust]
                       [:host  :replication :nicola "127.0.0.1/32" :trust]
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

(defn initdb [cfg]
  (log/info "[" (:name cfg) "] " "initdb -D" (pg-data-dir cfg))
  (let [res (sh/sh (bin "initdb") "-D" (pg-data-dir cfg))]
    (log/info (:out res))
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
        (state/put-in [:pg :process] (pid cfg))
        res))
    (log/info "Node not initialized")))

(defn stop []
  (pg_ctl (get-node) :stop))

(defn master [opts]
  (let [cfg (node-config opts)]
    (initdb cfg)
    (pg-config/update-config cfg)
    (pg-config/update-hba cfg)
    (state/put-in [:pg] cfg)
    (start)
    cfg))

(defn kill [pid sig]
  (sh/sh "kill" (str "-" (str/upper-case (name sig))) pid))


(defn replica [parent-cfg replica-opts]
  (let [cfg (node-config replica-opts)]
    (log/info (bin "pg_basebackup")
              "-h"  (pg-data-dir parent-cfg)
              "-p" (str (:port parent-cfg))
              "-c" "fast"
              "-D" (pg-data-dir cfg))
    (log/info (sh/sh (bin "pg_basebackup")
                     "-h" (pg-data-dir parent-cfg)
                     "-p" (str (:port parent-cfg))
                     "-c" "fast"
                     "-D" (pg-data-dir cfg)))
    (pg-config/update-config cfg)
    (pg-config/update-hba cfg)
    (pg-config/update-recovery parent-cfg cfg)
    (state/put-in [:pg] cfg)))


(comment
  (sh/sh "rm" "-rf" "/tmp/wallogs")
  (sh/sh "mkdir" "-p" "/tmp/wallogs/pg_xlog")

  (master {:name "node-1" :port 5434})

  @state
  (start "node-1")
  (stop "node-1")
  (replica "node-1" {:name "node-2" :port 5435})
  (start "node-2")
  (stop "node-2")

  (replica "node-1" {:name "node-3" :port 5436})
  (start "node-3")
  (stop "node-3"))
