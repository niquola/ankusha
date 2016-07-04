(ns ankusha.util
  (:require [clojure.core.async :as a :refer [>! <! go-loop alt! chan close! timeout]]
            [clojure.tools.logging :as log]
            [ankusha.state :as state]))

(defn stop-checker [nm]
  (when-let [c (state/get-in [:checkers nm])] (close! c)))

(defn start-checker [nm timeout-fn f]
  (let [stop (chan)]
    (go-loop []
      (let [tm (timeout (timeout-fn))]
        (alt!
          tm (do (f)
                 (recur))
          stop (log/info "Stop " nm))))
    (state/assoc-in [:checkers nm] stop)))

(defn xmethods [obj]
  (sort
   (map #(.getName %)
        (.getMethods (type obj)))))

(defn get-private-field [instance field-name]
  (. (doto (first (filter (fn [x] (.. x getName (equals field-name)))
                          (.. instance getClass getDeclaredFields)))
       (.setAccessible true))
     (get instance)))

(defn get-super-private-field [instance field-name]
  (. (doto (first (filter (fn [x] (.. x getName (equals field-name)))
                          (.. instance getClass getSuperclass getDeclaredFields)))
       (.setAccessible true))
     (get instance)))
