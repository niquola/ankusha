(ns ankusha.config
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.core.async :as async :refer [<!]]))

(defn pg-data-dir [cfg & args]
  (apply str (:data-dir cfg) "/pg" (when-not (empty? args) (str "/" (str/join "/" args)))))

(defn to-config [cfg]
  (with-out-str
    (doseq [[k v] cfg]
      (println (name k) " = " (if (string? v) (str "'" v "'") v)))))

(defn to-props [cfg]
  (str/join " " (for [[k v] cfg] (str (name k) "=" v))))

(to-props {:a 1 :b 3})

(defn mk-config [cfg]
  (to-config (:config cfg)))

(defn mk-hba [cfg]
  (with-out-str
    (doseq [ks (:hba cfg)]
      (println (str/join "\t" (map name ks))))))

(defn mk-recovery [master-cfg replica-cfg]
  (to-config
   {:standby_mode "on"
    :primary_conninfo (to-props {:host "localhost"
                                 :port (:port master-cfg)
                                 :user "nicola"
                                 :password "nicola"})
    :restore_command  (str "cp /tmp/wallogs/pg_xlog/%f %p")
    :archive_cleanup_command "rm -f %r"}))

(defn update-hba [cfg]
  (let [hba (mk-hba cfg)
        conf_path (pg-data-dir cfg "pg_hba.conf")]
    (log/info "Update pg_hbal:\n" hba)
    (spit conf_path hba)))

(defn update-config [cfg]
  (let [pgconf (mk-config cfg)
        conf_path (pg-data-dir cfg "postgresql.conf")]
    (log/info "Update config:\n" pgconf)
    (spit conf_path pgconf)))

(defn update-recovery [master-cfg replica-cfg]
  (let [txt (mk-recovery master-cfg replica-cfg)
        conf_path (pg-data-dir replica-cfg "/recovery.conf")]
    (log/info "Update recovery.conf [" conf_path "]\n" txt )
    (spit conf_path txt)))

;; (defn sighup-params [cfg]
;;   (pg/exec (conn-uri cfg)
;;            {:select [:name]
;;             :from [:pg_settings]
;;             :where [:= :context "sighup"]}))

(comment
  (update-config config)
  (reconfigure config))
