(ns ankusha.web
  (:require [cljs.nodejs :as node]
            [route-map.core :as route]))

(def http (node/require "http"))

(defn swagger [req resp]
  (.end resp "Swagger"))

(defn not-found [req resp]
  (let [method (.-method req)
        url (.-url req)]
    (.end resp (str  "Resource " method " " url " not found"))))

(defn nodes [req resp]
  (.end resp "Nodes list"))

(def routes
  {:GET #'swagger
   "nodes" {:GET #'nodes}})

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

