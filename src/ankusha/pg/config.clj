(ns ankusha.pg.config
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [ankusha.config :as conf]
            [clojure.core.async :as async :refer [<!]]))

(defn pg-data-dir [cfg & args]
  (apply str (:lcl/data-dir cfg) "/pg" (when-not (empty? args) (str "/" (str/join "/" args)))))

(defn to-config [cfg]
  (with-out-str
    (doseq [[k v] cfg]
      (println (name k) " = " (if (string? v) (str "'" v "'") v)))))

(defn to-props [cfg]
  (str/join " " (for [[k v] cfg] (str (name k) "=" v))))

(defn mk-config [cfg]
  (to-config (:config cfg)))

(defn hba [gcfg lcfg]
  (->> gcfg
       :glb/hba
       (map #(str/join "\t" %))
       (str/join "\n")))

(defn config [gcfg lcfg]
  (to-config (->
              (merge (:glb/postgres gcfg)
                     (:lcl/postgres lcfg)
                     {:unix_socket_directories (pg-data-dir lcfg) 
                      :log_line_prefix (str "<" (:lcl/name lcfg) ">")})
              (dissoc
               :pg/restore_command
               :pg/archive_cleanup_command))))

(defn recovery [gcfg pcfg lcfg]
  (to-config
   {:standby_mode "on"
    :primary_conninfo (to-props {:host (:lcl/host pcfg) 
                                 :port (get-in pcfg [:lcl/postgres :pg/port])
                                 :user  "replication" 
                                 :password (get-in gcfg [:glb/users :usr/replication :usr/password])})
    :restore_command (get-in lcfg [:lcl/postgres :pg/restore_command]) 
    :archive_cleanup_command (get-in lcfg [:lcl/postgres :pg/archive_cleanup_command])}))

(comment
  (conf/load-local "sample/node-1.edn")
  (conf/load-global "sample/config.edn")

  (println "--------")
  (println (hba (conf/global) (conf/local)))

  (println "--------")
  (println (config (conf/global) (conf/local)))

  (recovery (conf/global) (conf/local) (conf/local))
  
  )
