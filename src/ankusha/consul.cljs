(ns ankusha.consul
  (:require-macros [cljs.core.async.macros :as m :refer [go alt! go-loop]])
  (:require [cljs.nodejs :as node]
            [cljs.core.async :as async]))

(def Consul (node/require "consul"))

(def consul (Consul))

(defn wrap-async [scope f]
  (fn [& args]
    (let [ch (async/chan)
          cb (fn [err res]
               (if err
                 (do (.log js/console "ERROR:" err)
                     (async/close! ch))
                 (if res (async/put! ch (js->clj res)) (async/close! ch))))
          args (or (clj->js args) #js[])]
      (.push args cb)
      (.apply (aget scope f) scope args)
      ch)))

(def members (wrap-async (.-agent consul) "members"))

(def session (wrap-async (.-session consul) "get"))
(def create-session (wrap-async (.-session consul) "create"))
(def list-sessions (wrap-async (.-session consul) "list"))
(def destroy-session (wrap-async (.-session consul) "destroy"))
(def renew-session (wrap-async (.-session consul) "renew"))

(defn clear-sessions []
  (go
    (if-let [ss (async/<! (list-sessions))]
      (doseq [s ss]
        (println "Destroy " s)
        (println (async/<! (destroy-session (get s "ID"))))))))

(defonce current-session (atom nil))

(defn keep-session []
  (go
    (if-let [sess (and  @current-session (async/<! (session @current-session)))]
      (println "RENEW" (async/<! (renew-session (get sess "ID"))))
      (if-let [sess (async/<! (create-session {:ttl "10s"}))]
        (do (println "CREATE SESSION" sess)
            (reset! current-session (get sess "ID")))
        (println "UPS NOT CREATED")))))

(defonce watcher (atom nil))

(defn stop []
  (when-let [ch @watcher]
    (async/close! ch)
    (reset! watcher nil)))

(def timeout 5000)

(defn start []
  (let [stop (async/chan)]
    (go-loop []
      (alt!
        stop (println "Stop service")
        (async/timeout timeout) (do (async/<! (keep-session))
                                 (recur))))
    (reset! watcher stop)
    stop))

(comment 
  (keep-session)

  (start)
  (stop)

  (clear-sessions)

  @current-session
  
  (go (println (async/<! (destroy-session "928ef1a6-6591-9dd0-e108-786301f9b9b4"))))

  (go (println (async/<! (create-session {:ttl "10s"}))))

  (go (println (async/<! (list-sessions))))
  (go (println (async/<! (members))))
  )

