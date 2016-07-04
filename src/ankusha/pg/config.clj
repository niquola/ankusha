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
  (to-config (merge (:glb/postgres gcfg)
                    (:lcl/postgres lcfg)
                    {:unix_socket_directories (pg-data-dir lcfg) 
                     :log_line_prefix (str "<" (:lcl/name lcfg) ">")})))

(defn mk-recovery [master-cfg lcfg]
  (to-config
   {:standby_mode "on"
    :primary_conninfo (to-props {:host (:host master-cfg) 
                                 :port (:port master-cfg)
                                 :user (get-in master-cfg [:user :name]) 
                                 :password (get-in master-cfg [:user :password])})
    :restore_command (:pg/restore_command lcfg) 
    :archive_cleanup_command (:pg/archive_cleanup_command lcfg)}))

(comment
  (conf/load-local "sample/node-1.edn")
  (conf/load-global "sample/config.edn")

  (println "--------")
  (println (hba (conf/global) (conf/local)))

  (println "--------")
  (println (config (conf/global) (conf/local)))
  
  )
