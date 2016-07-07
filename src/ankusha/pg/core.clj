(ns ankusha.pg.core
  (:require [clojure.string :as str]
            [ankusha.pg.config :as pg-config :refer [pg-data-dir]]
            [ankusha.state :as state :refer [with-node]]
            [clojure.java.shell :as sh]
            [ankusha.log :as log]
            [clojure.java.io :as io]))

;; TODO:
;;   integrate pg_controldata

(def config (atom {:bin "/usr/lib/postgresql/9.5/bin"}))

(defn bin [util] (str (:bin @config) "/" util))

(defn sh! [& args]
  (log/info args)
  (let [res (apply sh/sh args)]
    (if (= 0 (:exit res))
      res
      (do (log/error res)
        (throw (Exception. (str res)))))))

(defn sh [& args]
  (log/info args)
  (apply sh/sh args))

(defn pg_ctl! [data-dir cmd & [args]]
  (apply sh! (bin "pg_ctl")
         "-D" data-dir
         "-l" (str data-dir "/postgres.log")
         (name cmd) args))

(defn pg_ctl [data-dir cmd & [args]]
  (apply sh (bin "pg_ctl")
         "-D" data-dir
         "-l" (str data-dir "/postgres.log")
         (name cmd) args))


(defn psql [data-dir port sql]
  (let [res (sh "psql" "postgres" "-h" data-dir "-p" (str port) "-c" sql)]
    (if (= 0 (:exit res))
      (log/info "psql:"  (pr-str (:out res)))
      (log/error "psql:" (pr-str (:err res))))
    res))

(defn psql! [data-dir port sql]
  (let [res (sh "psql" "postgres" "-h" data-dir "-p" (str port) "-c" sql)]
    (if (= 0 (:exit res))
      (log/info "psql:"  (pr-str (:out res)))
      (log/error "psql:" (pr-str (:err res))))
    res))

(defn wait-pg [data-dir port sec & [sql]]
  (loop [n sec]
    (if (> n 0)
      (let [res (psql data-dir port (or sql "SELECT 1"))]
        (when (not (= 0 (:exit res)))
          (Thread/sleep 1000)
          (recur (dec n))))
      (throw (Exception. (str "Unable to connect to postgres")))))
  (log/info "postgresql is up"))

(defn multiline [s]
  (->> #"\n"
       (str/split s)
       (map #(str "> " %))
       (str/join "\n")
       (str "\n")))

(defn initdb [data-dir]
  (log/info "initdb -D" data-dir)
  (let [res (sh/sh (bin "initdb") "-D" data-dir)]
    (if (= 0 (:exit res))
      (log/info (multiline (:out res)))
      (do (log/error (multiline (:err res)))
          (throw (Exception. (:err res)))))))

(defn pid [lcfg]
  (let [pth (pg-data-dir lcfg "/postmaster.pid")]
    (when (.exists (io/file pth))
      (first (str/split (slurp pth) #"\n")))))

(defn create-user [data-dir port users]
  (doseq [{nm   :usr/name
           pswd :usr/password
           spr  :usr/superuser} users]
    (psql data-dir port
          (str  "CREATE USER " (name nm) " WITH "
                (when spr " SUPERUSER ")
                " PASSWORD '" pswd "'"))))


(def recomended-config
  #:pg{:max_connections 100
       :archive_mode "on"
       :archive_timeout 60
       :max_wal_senders 10
       :wal_keep_segments 100
       :shared_buffers  "128MB"})

(def required-config
  #:pg{:listen_addresses "*"
       :wal_level "logical"
       :hot_standby "on"
       :wal_log_hints "on"
       :archive_mode "on"
       :full_page_writes "on"})

(defn build-hba [hba]
  (into [["local" "all" "all" "trust"]
         ["host"  "replication" "all" "127.0.0.1/32" "md5"]
         ["host"  "replication" "system" "0.0.0.0/0" "md5"]
         ["host"  "all" "system" "0.0.0.0/0" "md5"]]
        (or hba [])))

(defn update-config [data-dir conf]
  (spit (str  data-dir "/postgresql.conf")
        (pg-config/to-config
         recomended-config
         conf
         required-config
         {:pg/unix_socket_directories data-dir})))

(defn to-hba [hba]
  (->> hba 
       (map #(str/join "\t" %))
       (str/join "\n")))

(defn update-hba [data-dir hba]
  (spit (str data-dir "/pg_hba.conf")
        (to-hba (build-hba hba))))

(defn master
  [{data-dir :data-dir
    {port :pg/port :as conf} :config
    hba :hba
    users :users}]
  (initdb data-dir)
  (update-config data-dir conf)
  (update-hba data-dir hba)
  (pg_ctl! data-dir :start)
  (wait-pg data-dir port 10 "SELECT 1")
  (create-user data-dir port users))


(defn kill [pid sig]
  (sh/sh "kill" (str "-" (str/upper-case (name sig))) pid))

;; -Ft -x

(defn base-backup
  [data-dir
   {host :host
    port :port
    user :user
    password :password}]
  (let [pgpass-path (str "/tmp/.pgpass")]
    (sh! "mkdir" "-p" data-dir)
    (spit pgpass-path (str host ":" port ":*:" user ":" password "\n"))
    (sh! "chmod" "0600" pgpass-path)
    (sh! (bin "pg_basebackup")
         "-w" "-x" "-h" host "-p" (str port) "-U" user
         "-c" "fast" "-D" data-dir
         :env {"PGPASSFILE" pgpass-path})
    (sh! "rm" "-f" pgpass-path)))


(defn init-replica
  [primary-conn
   {data-dir :data-dir
    timeout :timeout
    conf :config
    hba :hba}]

  (update-config data-dir (dissoc conf :pg/restore_command :pg/archive_cleanup_command))
  (update-hba data-dir hba)

  (spit (str data-dir "/recovery.conf")
        (pg-config/recovery
         (merge {:primary_conninfo primary-conn}
                (select-keys conf [:pg/restore_command
                                   :pg/archive_cleanup_command]))))

  (pg_ctl! data-dir :start)
  (wait-pg data-dir (:pg/port conf) (or timeout 600) "SELECT 1"))

(defn replica [primary-conn
               {data-dir :data-dir
                timeout :timeout
                conf :config
                hba :hba :as opts}]
  (sh! "mkdir" "-p" data-dir)
  (sh! "chmod" "0700" data-dir)
  (base-backup data-dir primary-conn)
  (init-replica primary-conn opts))

(defn promote [data-dir port]
  (pg_ctl! data-dir :promote)
  (wait-pg data-dir port 10 "SELECT 1"))

(defn libpq-conn-string
  [{user :user password :password host :host port :port}]
  (str "postgresql://" user ":" password "@" host ":" port "/postgres"))

(defn pg_rewind [primary-conn data-dir]
  (sh! (bin "pg_rewind")
       "-D" data-dir
       "--source-server" (libpq-conn-string primary-conn)))

(defn switch [primary-conn {data-dir :data-dir timeout :timeout conf :config :as opts}]
  (pg_ctl data-dir :stop)
  (pg_rewind primary-conn data-dir)
  (init-replica primary-conn opts))

(comment
  pg_receivexlog !!!!!!!!!!!!!!!!!!
  )

(comment
  (initdb "/tmp/pg/master")
  (do
    (pg_ctl "/tmp/pg/master" :stop)
    (pg_ctl "/tmp/pg/replica" :stop)
    (sh "rm" "-rf" "/tmp/pg/master")
    (sh "rm" "-rf" "/tmp/pg/pg_xlog")
    (sh "rm" "-rf" "/tmp/pg/replica")
    (sh "mkdir" "-rf" "/tmp/pg/pg_xlog"))

  (def master-conf
    #:pg{:port 5434
         :archive_command "cp %p /tmp/pg/pg_xlog"
         :log_line_prefix "node-1"})

  (def master-conn
    {:user "system"
     :password "system"
     :host "localhost"
     :port 5434})

  (def replica-conf
    {:data-dir "/tmp/pg/replica"
     :config #:pg{:port 7000
                  :restore_command "cp /tmp/pg/pg_xlog/%f %p"}})

  (def replica-conn
    {:user "system"
     :password "system"
     :host "localhost"
     :port 7000})

  (pg_ctl! "/tmp/pg/master" :stop "-m" "fast")

  (pg_ctl! "/tmp/pg/master" :start)

  (master {:data-dir "/tmp/pg/master"
           :users [#:usr{:name "system"
                         :superuser true
                         :password "system"}]
           :config master-conf})

  (sh "rm" "-rf" "/tmp/pg/replica")

  (replica master-conn replica-conf)

  (promote "/tmp/pg/replica" 7000)

  (switch replica-conn
          {:data-dir "/tmp/pg/master"
           :port 5434
           :config master-conf
           :timeout 30})

  (promote "/tmp/pg/master" 5434)

  (switch master-conn
          {:data-dir "/tmp/pg/replica"
           :config replica-conf 
           :timeout 30})

  (pg_ctl "/tmp/pg/replica" :stop)
  (pg_ctl "/tmp/pg/replica" :start)


  (base-backup "/tmp/pg/base" master-conn)

  (sh "rm" "-rf" "/tmp/pg/replica")

  )



