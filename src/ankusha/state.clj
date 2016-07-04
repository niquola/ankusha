(ns ankusha.state
  (:refer-clojure :exclude [get-in assoc-in]))

(defonce state (atom {}))

(def ^:dynamic *current* "node-1")

(defn write [val]
  (swap! state assoc *current* val))

(defn read [val] (get state *current*))

(defn assoc-in [path val]
  (swap! state clojure.core/assoc-in (into [*current*] path) val))

(defn get-in [path]
  (clojure.core/get-in @state (into [*current*] path)))

(defn current [] *current*)

(defmacro with-node [node & body]
  `(binding [*current* ~node] ~@body))

