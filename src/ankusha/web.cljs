(ns ankusha.web
  (:require [cljs.nodejs :as node]
            [ankusha.swagger :as swagger]
            [ankusha.pg-cluster :as pg]
            [route-map.core :as route]))

(def http (node/require "http"))

(declare routes)

(defn responce [resp m]
  (aset resp "statusCode" (or (:status m) 200))
  (.setHeader resp "conent-type" "application/json")
  (.end resp (.stringify js/JSON (clj->js (:body m)) nil 4)))

(defn swagger
  {:swagger {:summary "Swagger spec for API"}}
  [req resp]
  (responce resp {:body (swagger/spec routes)}))

(defn not-found [req resp]
  (let [method (.-method req)
        url (.-url req)]
    (responce resp {:status 404
                    :body {:message (str  "Resource " method " " url " not found")}})))

(defn status
  {:swagger {:summary "List nodes in cluster"}}
  [req resp]
  (responce resp {:body [{:name "node-1" :status "master" :ip ""}
                         {:name "node-2" :status "replica" :ip ""}]}))

(defn local-status
  {:swagger {:summary "Status concerete node"}}
  [req resp]
  (responce resp {:body {:status (pg/status)}}))

(defn register
  {:swagger {:summary "Register node in cluster"}}
  [req resp]
  (responce resp {:body {:message "Success"}}))

(def routes
  {:GET #'swagger
   "status" {:GET #'local-status}
   "cluster" {"status" {:GET #'status}
              "register" {:GET #'register}}})

(defn handler [req resp]
  (let [method (.-method req)
        url (.-url req)]
    (if-let [rt (route/match [(keyword method) url] routes)]
      ((:match rt) req resp)
      (not-found req resp))))

(defonce server (atom nil))

(defn stop []
  (when-let [s @server]
    (.close s (fn [_] (println "HTTP server stopped")))))

(defn start []
  (let [port 8651
        srv (.createServer http (fn [req resp] (handler req resp)))]
    (reset! server srv)
    (.listen srv port (fn [_])
             (println "Start server on " port))))

(comment
  (start)
  (stop)
  @server
  )

