(ns ankusha.web
  (:require [ankusha.swagger :as swagger]
            [ankusha.consensus :as cluster]
            [ankusha.state :as state]
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
  (json (cluster/dmap! "nodes")))

(defn status
  {:swagger {:summary "List nodes in cluster"}}
  [req]
  (json (cluster/status)))

(defn local-status
  {:swagger {:summary "Status concerete node"}}
  [req]
  (json {:status "TODO"}))

(defn register
  {:swagger {:summary "Register node in cluster"}}
  [req]
  (json {:message "Success"} 201))

(def routes
  {:GET #'swagger
   "status" {:GET #'local-status}
   "postgres" {"status" {:GET #'pg-status}}
   "cluster" {"status" {:GET #'status}
              "register" {:GET #'register}}})

(defn handler [{url :uri method :request-method :as req}]
  (if-let [rt (route/match [(keyword method) url] routes)]
    ((:match rt) (update-in req [:params] merge (:params rt)))
    (not-found req)))

(defn stop []
  (when-let [s (state/get-in [:web])] (s)))

(defn start [port]
  (stop)
  (let [srv (http/run-server #'handler {:port port})]
    (state/assoc-in [:web] srv)))

(comment
  (start 8888)
  (stop)
  )

