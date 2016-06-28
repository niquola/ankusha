(ns ankusha.pg-cluster
  (:require-macros [cljs.core.async.macros :as m :refer [go alt! go-loop]])
  (:require [cljs.nodejs :as node]
            [clojure.string :as str]
            [ankusha.shell :as shell]
            [ankusha.pg :as pg]
            [cljs.core.async :as async :refer [<!]]))

(def proc (node/require "child_process"))

(defonce settings (atom {:bin "/usr/lib/postgresql/9.5/bin"}))

(defn bin [util] (str (:bin @settings) "/" util))

(defonce pg-proc (atom nil))
(defonce pg-on-exit (atom nil))

(def config {:data-dir "/tmp/cluster-1"
             :config {:max_connections 100
                      :listen_addresses "*"
                      :unix_socket_directories "/tmp/cluster-1"

                      :wal_level "logical"
                      :archive_mode "on"
                      :archive_command "rm -rf %" 
                      :archive_timeout 60

                      :shared_buffers  "128MB"
                      :port "5434"}})

(defn mk-config [cfg]
  (with-out-str
    (doseq [[k v] (:config cfg)]
      (println (name k) " = " (if (string? v) (str "'" v "'") v)))))

(defn update-config [cfg]
  (let [pgconf (mk-config cfg)
        conf_path (str (:data-dir cfg) "/postgresql.conf")]
    (println "Update config:\n" pgconf)
    (shell/spit conf_path pgconf)))

(defn start-postgres [cfg]
  (let [chan (async/chan)
        p (.spawn proc (bin "postgres") #js["-D" (:data-dir cfg)])]
    (.pipe (.-stdout p ) (.-stdout js/process))
    (.pipe (.-stderr p) (.-stderr js/process))
    (.on p "exit" (fn [code sig]
                    (println "Postgres exit with " code " SIG " sig)
                    (when code (async/put! chan code))
                    (async/close! chan)))

    (reset! pg-proc p)
    (reset! pg-on-exit chan)
    chan))

(defn stop-postgres [sig]
  (when-let [proc @pg-proc]
    (.kill proc (str/upper-case (name sig)))))

(defn restart-postgres [cfg]
  (stop-postgres :sigint)
  (start-postgres cfg))

(defn reconfigure [cfg]
  (go
    (println "CONFIG:" (<! (update-config cfg)))
    (println "RESTART:" (<! (restart-postgres cfg)))))

(defn pg_ctl [cfg cmd]
  (shell/spawn (bin "pg_ctl") {:-D (:data-dir cfg)
                               :-l (str (:data-dir cfg) "/postgres.log")}
               (name cmd)))

(defn stop-by-pid [pid]
  )

(defn postmaster-pid [cfg]
  (go (if-let [res (<! (shell/slurp (str (:data-dir cfg) "/postmaster.pid")))]
        (first (str/split  res #"\n"))
        nil)))

(defn sighup-params [cfg]
  (pg/exec (conn-uri cfg)
           {:select [:name]
            :from [:pg_settings]
            :where [:= :context "sighup"]}))

(defn conn-uri [cfg]
  (str "postgres:///postgres?host=" (:data-dir cfg) "&port=" (get-in cfg [:config :port])))

(defn probe-connection [cfg]
  (pg/exec (conn-uri cfg) {:select [1]}))

(defn create-cluster [cfg]
  (let [cmd []]
    (go
      (<! (shell/spawn "rm" "-rf" (:data-dir cfg)))
      (pg_ctl cfg :stop)
      (println "Cluster creation status " (<! (pg_ctl cfg :initdb)))
      (println "Write config" (<! (update-config cfg)))
      (println "Start cluster" (<! (start-postgres cfg)))
      (println "Probe connection" (<! (probe-connection config))))))



(comment
  (update-config config)
  (reconfigure config)

  (create-cluster config)

  (goprint (postmaster-pid config ))

  (goprint
   (start-postgres config))

  (stop-postgres :sigint)

  (.kill @pg-proc "SIGHUP")
  (.kill @pg-proc "SIGINT")

  (.kill @pg-proc "SIGKILL")

  (goprint (probe-connection config))
  (goprint (shell/slurp "/tmp/cluster-1/postmaster.pid"))

  (goprint (sighup-params config))
  )

;; SIGTERM This is the Smart Shutdown mode. After receiving SIGTERM, the server
;; disallows new connections, but lets existing sessions end their work normally.
;; It shuts down only after all of the sessions terminate. If the server is in
;; online backup mode, it additionally waits until online backup mode is no longer
;; active. While backup mode is active, new connections will still be allowed, but
;; only to superusers (this exception allows a superuser to connect to terminate
;; online backup mode). If the server is in recovery when a smart shutdown is
;; requested, recovery and streaming replication will be stopped only after all
;; regular sessions have terminated.

;; SIGINT This is the Fast Shutdown mode. The server disallows new connections and
;; sends all existing server processes SIGTERM, which will cause them to abort
;; their current transactions and exit promptly. It then waits for all server
;; processes to exit and finally shuts down. If the server is in online backup
;; mode, backup mode will be terminated, rendering the backup useless.

;; SIGQUIT This is the Immediate Shutdown mode. The server will send SIGQUIT to all
;; child processes and wait for them to terminate. If any do not terminate within 5
;; seconds, they will be sent SIGKILL. The master server process exits as soon as
;; all child processes have exited, without doing normal database shutdown
;; processing. This will lead to recovery (by replaying the WAL log) upon next
;; start-up. This is recommended only in emergencies.

