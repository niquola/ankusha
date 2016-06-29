(ns ankusha.pg-cluster
  (:require-macros [cljs.core.async.macros :as m :refer [go alt! go-loop]])
  (:require [cljs.nodejs :as node]
            [clojure.string :as str]
            [ankusha.shell :as shell]
            [ankusha.pg-config :as pg-config]
            [ankusha.pg :as pg]
            [cljs.core.async :as async :refer [<!]]))

(def proc (node/require "child_process"))

(defonce state (atom {:bin "/usr/lib/postgresql/9.5/bin"
                      :nodes {}}))

(defn bin [util] (str (:bin @state) "/" util))

(defn node-config [opts]
  {:data-dir (:data-dir opts)
   :port (:port opts)
   :hba [[:local :all :all :trust]
         [:local :replication :nicola :trust]
         [:host  :replication :nicola "127.0.0.1/32" :trust]
         [:host :all :all "0.0.0.0/0" :md5]]
   :config {:max_connections 100
            :listen_addresses "*"
            :unix_socket_directories (:data-dir opts)

            :wal_level "logical"
            :hot_standby "on"
            :archive_mode "on"
            :archive_command "cp %p /tmp/wallogs/%p" 
            :archive_timeout 60
            :max_wal_senders 2
            :wal_keep_segments 100

            :shared_buffers  "128MB"
            :log_line_prefix  (str (:name opts) ": ")

            :port (:port opts)}})

(defn pg_ctl [cfg cmd]
  (shell/spawn (bin "pg_ctl") {:-D (:data-dir cfg) :-l (str (:data-dir cfg) "/postgres.log")} (name cmd)))

(defn initdb [cfg] (pg_ctl cfg :initdb))

(defn create-cluster [opts]
  (let [cfg (node-config opts)]
    (go (println "Clear directory" (<! (shell/spawn "rm" "-rf" (:data-dir cfg))))
        (println "Cluster creation status " (<! (initdb cfg)))
        (println "Write config" (<! (pg-config/update-config cfg)))
        (println "Write hba" (<! (pg-config/update-hba cfg)))
        (swap! state assoc-in [:nodes (:name opts)] cfg))))

(defn *start [node-name cfg chan]
  (let [p (.spawn proc (bin "postgres") #js["-D" (:data-dir cfg)])]
    (println "SPAWN: postgres -D " (:data-dir cfg))
    (.pipe (.-stdout p ) (.-stdout js/process))
    (.pipe (.-stderr p) (.-stderr js/process))
    (.on p "exit" (fn [code sig]
                    (println "Postgres exit with " code)
                    (when code (async/put! chan code))
                    (async/close! chan)))
    (swap! state assoc-in [:nodes node-name :process] p)))

(defn start-postgres [node-name]
  (let [cfg (get-in @state [:nodes node-name])
        pg-proc (get-in @state [:nodes node-name :process])
        chan (async/chan)]
    (cond
      (and cfg pg-proc) (do (println "Postgresql already started " (.-pid pg-proc)) (async/close! chan))
      cfg (do (println "Starging pg cluster" cfg)
              (*start node-name cfg chan))
      :else (do (println "No such node: " node-name)
                (async/close! chan)))
    chan))

(defn stop-postgres [node-name sig]
  (if-let [proc (get-in @state [:nodes node-name :process])]
    (do
      (println "Stopping pg cluster")
      (.kill proc (str/upper-case (name sig)))
      (swap! state assoc-in [:nodes node-name :process] nil))
    (println "Postgresql already stopped")))

(defn create-replica [conn replica-opts]
  ;; pg_basebackup -h `pwd` -p 5434 -c fast -D /tmp/cluster-2
  (go
    (let [cfg (node-config replica-opts)]
      (println "Clear directory" (<! (shell/spawn "rm" "-rf" (:data-dir cfg))))
      (<! (shell/spawn "pg_basebackup"
                       {:-h (:data-dir conn)
                        :-p (:port conn)
                        :-c "fast"
                        :-D (:data-dir cfg)}))
      (println "config" (<! (pg-config/update-config cfg)))
      (println "hba" (<! (pg-config/update-hba cfg)))
      (println "recovery" (<! (pg-config/update-recovery conn cfg)))
      (swap! state assoc-in [:nodes (:name replica-opts)] cfg))))


(defn goprint [x]
  (go (println (<! x))))

(comment

  (goprint (shell/spawn "rm" "-rf" "/tmp/wallogs"))
  (goprint (shell/spawn "mkdir" "-p" "/tmp/wallogs/pg_xlog"))

  (reset! state {:bin "/usr/lib/postgresql/9.5/bin" :nodes {}})

  (create-cluster {:name "node-1" :port 5434 :data-dir "/tmp/node-1"})

  (keys (:nodes @state))

  (goprint (start-postgres "node-1"))
  (stop-postgres "node-1" :sigint)


  (create-replica {:name "node-1" :port 5434 :data-dir "/tmp/node-1"}
                  {:name "node-2" :port 5435 :data-dir "/tmp/node-2"})

  (goprint (start-postgres "node-2"))
  (stop-postgres "node-2" :sigint)
  )

(comment
  (defnl reload-config [node-name]
    (.kill @pg-proc "SIGHUP"))

  (defn stop-postgres-by-pid [cfg]
    (go
      (when-let [pid (<! (postmaster-pid cfg))]
        (println "Killing postgres" (<! (shell/spawn "kill" "-9" pid)))
        (println "Remove pid file" (<! (shell/spawn "rm" "-f" (pid-file cfg))))
        (reset! pg-proc nil))))


  (defn conn-uri [cfg]
    (str "postgres:///postgres?host=" (:data-dir cfg) "&port=" (get-in cfg [:config :port])))

  (defn pid-file [cfg] (str (:data-dir cfg) "/postmaster.pid"))

  (defn postmaster-pid [cfg]
    (go (if-let [res (<! (shell/slurp (pid-file cfg)))]
          (first (str/split  res #"\n"))
          nil)))

  (defn probe-connection [cfg]
    (pg/exec (conn-uri cfg) {:select [1]})))




