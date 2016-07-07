(ns ankusha.log
  (:require [clojure.tools.logging :as log]
            [ankusha.config :as config]))

(defn node-name []
  (str "[" (:lcl/name (config/local)) "]"))

(defmacro warn [& args]
  `(log/warn (node-name) ~@args))

(defmacro info [& args]
  `(log/info (node-name) ~@args))

(defmacro debug [& args]
  `(log/debug (node-name) ~@args))

(defmacro error [& args]
  `(log/error (node-name) ~@args))


