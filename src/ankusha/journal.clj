(ns ankusha.journal
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.java.shell :as sh]
            [ankusha.state :as state]
            [ankusha.util :as util])
  (:import
   (journal.io.api JournalBuilder Journal Journal$WriteType Journal$ReadType)))

(defn encode [v]
  (String. (pr-str v)))

(defn decode [s]
  (edn/read-string s))

(defn open [dir]
  (state/assoc-in [:journal]
                  (.open (JournalBuilder/of (io/file dir)))))

(defn get-journal []
  (state/get-in [:journal]))

(defn close []
  (when-let [jrn (get-journal)]
    (.close jrn)))

(defn write [entry]
  (if-let [jrn (get-journal)]
    (.write jrn (.getBytes (encode (assoc entry :ts (util/now)))) Journal$WriteType/SYNC)
    (throw (Exception. "Journal not initialized"))))

(defn jreduce [f acc]
  (if-let [jrn (get-journal)]
    (reduce
     (fn [acc loc]
       (f acc (decode (String. (.read jrn loc Journal$ReadType/SYNC)))))
     acc
     (.redo jrn))
    (throw (Exception. "Journal not initialized"))))

(defn jmap [f]
  (jreduce (fn [acc x] (conj acc (f x))) []))

(defn start [lcfg]
  (let [jdir (str (:lcl/data-dir lcfg) "/journal")]
    (when-not (.exists (io/file jdir))
      (sh/sh "mkdir" "-p" jdir))
    (open jdir)))

(comment

  (require '[clojure.java.shell :as sh])

  (sh/sh "rm" "-rf" "/tmp/jrn")

  (sh/sh "mkdir" "/tmp/jrn")

  (open "/tmp/jrn")

  (write {:var 1})
  (write {:var 2})
  (write {:var 3})

  (jmap identity)
  (jreduce merge {})

  )

