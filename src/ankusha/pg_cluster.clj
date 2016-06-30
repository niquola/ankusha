(ns ankusha.pg-cluster
  (:require [clojure.string :as str]
            [ankusha.config :as pg-config]
            [ankusha.pg :as pg]
            [clojure.java.shell :as sh]
            [clojure.core.async :as async :refer [go alt! go-loop <!]]
            [clojure.java.io :as io]))

(defonce state (atom {:bin "/usr/lib/postgresql/9.5/bin"
                      :nodes {}}))

(defn bin [util] (str (:bin @state) "/" util))

(defn node-config [opts]
  (let [data-dir (or (:data-dir opts)
                     (str "/tmp/" (:name opts)))]
    {:data-dir data-dir 
     :port (:port opts)
     :hba [[:local :all :all :trust]
           [:local :replication :nicola :trust]
           [:host  :replication :nicola "127.0.0.1/32" :trust]
           [:host :all :all "0.0.0.0/0" :md5]]
     :config {:max_connections 100
              :listen_addresses "*"
              :unix_socket_directories data-dir 

              :wal_level "logical"
              :hot_standby "on"
              :archive_mode "on"
              :archive_command "cp %p /tmp/wallogs/%p" 
              :archive_timeout 60
              :max_wal_senders 2
              :wal_keep_segments 100

              :shared_buffers  "128MB"
              :log_line_prefix  (str (:name opts) ": ")

              :port (:port opts)}}))

(defn get-node [node-name]
  (get-in @state [:nodes node-name]))

(defn spawn [& args]
  (let [ex (-> (java.lang.ProcessBuilder. args)
               (.inheritIO)
               (.start))] ex))

(defn pg_ctl [cfg cmd]
  (sh/sh (bin "pg_ctl") "-D" (:data-dir cfg) "-l" (str (:data-dir cfg) "/postgres.log") (name cmd)))

(defn initdb [cfg]
  (.waitFor (spawn (bin "initdb") "-D" (:data-dir cfg))))

(defn pid [node-name]
  (let [cfg (get-node node-name)
        pth (str (:data-dir cfg) "/postmaster.pid")]
    (when (.exists (io/file pth))
      (first (str/split (slurp pth) #"\n")))))

(defn master [opts]
  (let [cfg (node-config opts)]
    (sh/sh "rm" "-rf" (:data-dir cfg))
    (initdb cfg)
    (pg-config/update-config cfg)
    (pg-config/update-hba cfg)
    (swap! state assoc-in [:nodes (:name opts)] cfg)))

(defn start [node-name]
  (if-let [cfg (get-node node-name)]
    (if (.exists (io/file (str (:data-dir cfg) "/postmaster.pid")))
      (println "Already started")
      (let [res (pg_ctl cfg :start)]
        (swap! state assoc-in [:nodes node-name :process] (pid cfg))
        res))
    (println "No such node" node-name)))

(defn kill [pid sig]
  (sh/sh "kill" (str "-" (str/upper-case (name sig))) pid))

(defn stop [node-name]
  (pg_ctl (get-node node-name) :stop))

(defn replica [node-name replica-opts]
  (let [parent-cfg (get-node node-name)
        cfg (node-config replica-opts)]
    (sh/sh "rm" "-rf" (:data-dir cfg))
    (.waitFor
     (spawn (bin "pg_basebackup")
            "-h" (:data-dir parent-cfg)
            "-p" (str (:port parent-cfg))
            "-c" "fast"
            "-D" (:data-dir cfg)))
    (pg-config/update-config cfg)
    (pg-config/update-hba cfg)
    (pg-config/update-recovery parent-cfg cfg)
    (swap! state assoc-in [:nodes (:name replica-opts)] cfg)))


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
