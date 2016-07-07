(ns ankusha.config
  (:require [ankusha.state :as state]
            [clojure.spec :as s]
            [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn file-exists? [fl] (.exists (io/file fl)))
(defn file-path? [fl] (re-matches #"/.*" fl))
(defn ipv4? [x] (re-matches #"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$" x))

(s/def :web/port number?)
(s/def :lcl/host ipv4?)

(s/def :lcl/postgres-bin file-exists?)
(s/def :lcl/data-dir file-path?)
(s/def :lcl/web (s/keys :req [:web/port]))
(s/def :lcl/atomix (s/keys :req [:ax/port]))
(s/def :lcl/postgres  (s/keys :req [:pg/port :pg/archive_command :pg/restore_command :pg/archive_cleanup_command]))

(s/def :glb/postgres  (s/keys :opt [:pg/archive_command :pg/port]))
(s/def :glb/lifebit  (s/keys :req [:tx/timeout]))
(s/def :glb/recovery  (s/keys :req [:tx/timeout]))


(s/def :lcl/config (s/keys :req [:lcl/data-dir :lcl/name :lcl/postgres-bin :lcl/postgres :lcl/host]))
(s/def :glb/config (s/keys :req [:glb/hba :glb/users :glb/postgres :glb/lifebit]))

(defn load-config [filepath]
  (edn/read-string (slurp filepath)))

(defn load-global [filepath]
  (let [cfg (load-config filepath)]
    (if (s/valid? :glb/config cfg)
      (do (state/assoc-in [:global-config] cfg) cfg)
      (throw (Exception. (with-out-str (s/explain :glb/config cfg)))))))

(defn load-local [filepath]
  (let [cfg (load-config filepath)]
    (if (s/valid? :lcl/config cfg)
      (do (state/assoc-in [:local-config] cfg) cfg)
      (throw (Exception. (with-out-str (s/explain :lcl/config cfg)))))))

(defn local []
  (state/get-in [:local-config]))

(defn global []
  (state/get-in [:global-config]))


(comment

  (load-local "sample/node-1.edn")
  (load-local "sample/node-2.edn")

  (load-global "sample/config.edn")



  )
