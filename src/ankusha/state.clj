(ns ankusha.state)

(defonce state (atom {}))

(def ^:dynamic *current* "node-1")

(defn write [val]
  (swap! state assoc *current* val))

(defn read [val] (get state *current*))

(defn put-in [path val]
  (swap! state assoc-in (into [*current*] path) val))

(defn get-in [path val]
  (get-in @state (into [*current*] path)))

(defn current [] *current*)

(defmacro with-node [node & body]
  `(binding [*current* ~node] ~@body))


