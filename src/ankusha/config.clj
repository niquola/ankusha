(ns ankusha.config
  (:require [ankusha.state :as state]
            [clojure.spec :as s]
            [clojure.edn :as edn]
            [clojure.java.io :as io]))

(defn file-exists? [fl] (.exists (io/file fl)))
(defn file-path? [fl] (re-matches #"/.*" fl))
(defn ipv4? [x] (re-matches #"^(?:[0-9]{1,3}\.){3}[0-9]{1,3}$" x))

(s/def :web/port number?)
(s/def :ax/host ipv4?)
(s/def :pg/host ipv4?)

(s/def :cfg/postgres-bin file-exists?)
(s/def :cfg/data-dir file-path?)
(s/def :cfg/web (s/keys :req [:web/port]))
(s/def :cfg/atomix (s/keys :req [:ax/port :ax/host]))
(s/def :cfg/postgres (s/keys :req [:pg/port :pg/host] :opt [:pg/archive_command]))


(s/def :cfg/local-config (s/keys :req [:cfg/data-dir :cfg/name :cfg/postgres-bin :cfg/postgres]))

(defn load-config [filepath]
  (edn/read-string (slurp filepath)))

(defn load-global [filepath]
  (edn/read-string (slurp filepath)))

(defn load-local [filepath]
  (let [cfg (load-config filepath)]
    (if (s/valid? :cfg/local-config cfg)
      (do
        (state/assoc-in [:local-config] cfg)
        cfg)
      (throw (Exception. (with-out-str (s/explain :cfg/local-config cfg)))))))

(defn local []
  (state/get-in [:local-config]))


(defn get-local [])

(comment

  (with-out-str
    (s/explain ::local-config
               (load-config "sample/node-1.edn")))

  (load-config "sample/node-1.edn")

  (load-local "sample/node-1.edn")

  (local)



  )
