(ns ankusha.web
  (:require [route-map.swagger :as swagger]
            [ankusha.atomix.core :as ax]
            [ankusha.state :as state]
            [ankusha.config :as config]
            [org.httpkit.server :as http]
            [cheshire.core :as json]
            [route-map.core :as route]))

(declare routes)

(defn json [body & [status]]
  {:body (json/generate-string body)
   :headers {"content-type" "application/json"}
   :status (or status 200)})

(defn swagger
  {:swagger {:summary "Swagger spec for API"}}
  [req]
  (json (swagger/spec routes)))

(defn not-found [{url :uri method :request-method}]
  (json {:message (str  "Resource " method " " url " not found")} 404))

(defn pg-status
  {:swagger {:summary "List nodes in cluster"}}
  [req]
  (json (ax/dmap! "nodes")))

(defn global-config
  {:swagger {:summary "Cluster global config"}}
  [req]
  (json (ax/dvar! "config")))

(defn health
  {:swagger {:summary "List nodes in cluster"}}
  [req]
  (json (ax/dmap! "nodes-health")))

(defn status
  {:swagger {:summary "List nodes in cluster"}}
  [req]
  (json (ax/status)))

(defn master
  {:swagger {:summary "List nodes in cluster"}}
  [req]
  (json (ax/dvar! "master")))

(defn connection
  {:swagger {:summary "List nodes in cluster"}}
  [req]
  (let [master (ax/dvar! "master")]
    (json {:host (:host master)
           :port (:port master)
           :user (get-in master [:user :name])
           :password (get-in master [:user :password])})))

(defn local-status
  {:swagger {:summary "Status concerete node"}}
  [req]
  (json (ax/dmap-get "nodes-health" (state/current))))

(defn register
  {:swagger {:summary "Register node in ax"}}
  [req]
  (json {:message "Success"} 201))


(defn local-config
  {:swagger {:summary "..."}}
  [req]
  (json (config/local)))

(def routes
  {:GET #'swagger
   "local" {"config" {:GET #'local-config}}
   "global" {"config" {:GET #'global-config}}
   "status" {:GET #'local-status}
   "postgres" {"status" {:GET #'pg-status}
               "health" {:GET #'health}
               "master" {:GET #'master
                         "connection" {:GET #'connection}}}
   "cluster" {"status" {:GET #'status}
              "register" {:GET #'register}}})

(defn handler [{url :uri method :request-method :as req}]
  (if-let [rt (route/match [(keyword method) url] routes)]
    ((:match rt) (update-in req [:params] merge (:params rt)))
    (not-found req)))

(defn stop []
  (when-let [s (state/get-in [:web])] (s)))

(defn start [lcfg]
  (stop)
  (let [port (get-in lcfg [:lcl/web :web/port])
        srv (http/run-server #'handler {:port port})]
    (state/assoc-in [:web] srv)))

(comment
  (start 8888)
  (stop))

