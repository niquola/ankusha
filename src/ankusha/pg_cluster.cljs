(ns ankusha.pg-cluster
  (:require-macros [cljs.core.async.macros :as m :refer [go alt!]])
  (:require [cljs.nodejs :as node]
            [ankusha.shell :as shell]
            [ankusha.pg :as pg]
            [cljs.core.async :as async :refer [<!]]))

(def proc (node/require "child_process"))

(def pg-bin-dir "/usr/lib/postgresql/9.5/bin")

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
        bin (str pg-bin-dir "/postgres")
        p (.spawn proc bin #js["-D" (:data-dir cfg)])]
    (.pipe (.-stdout p ) (.-stdout js/process))
    (.pipe (.-stderr p) (.-stderr js/process))
    (.on p "exit" (fn [code]
                    (println "Postgres exit with " code)
                    (async/put! chan code)
                    (async/close! chan)))

    (reset! pg-proc p)
    (reset! pg-on-exit chan)
    chan))

(defn stop-postgres []
  (when-let [proc @pg-proc]
    (.kill proc)))

(defn restart-postgres [cfg]
  (stop-posgres)
  (start-postgres cfg))

(defn reconfigure [cfg]
  (go
    (println "CONFIG:" (<! (update-config cfg)))
    (println "RESTART:" (<! (restart-postgres cfg)))))

(defn pg_ctl [cfg cmd]
  (let [bin (str pg-bin-dir "/pg_ctl")]
    (shell/spawn bin {:-D (:data-dir cfg)
                      :-l (str (:data-dir cfg) "/postgres.log")}
                 (name cmd))))

(defn conn-uri [cfg]
  (str "postgres:///postgres?host=" (:data-dir cfg) "&port=" (get-in cfg [:config :port])))



(defn probe-connection [cfg]
  (pg/exec (conn-uri cfg) {:select [1]}))


(defn create-cluster [cfg]
  (let [cmd []]
    (go
      (<! (shell/spawn "rm" "-rf" (:data-dir cfg)))
      (pg_ctl cfg :stop)
      (let [ch (pg_ctl cfg :initdb)
            res (<! ch)]
        (println "Cluster creation status " res)
        (println "Write config" (<! (update-config cfg)))
        (println "Start cluster" (<! (pg_ctl cfg :start)))))))

(comment
  (goprint (pg_ctl config :stop))
  (goprint (pg_ctl config :restart))

  (goprint (pg_ctl config :start))
  (goprint (pg_ctl config :status))

  (update-config config)

  (create-cluster config)

  (reconfigure config)

  (goprint (start-postgres config))

  (stop-postgres)

  (.kill @pg-proc 15)

  (goprint (probe-connection config))

  )
