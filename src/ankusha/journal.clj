(ns ankusha.journal
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn])
  (:import
   (journal.io.api JournalBuilder Journal Journal$WriteType Journal$ReadType)))

(defn encode [v]
  (String. (pr-str v)))

(defn decode [s]
  (edn/read-string s))

(defonce journal (atom nil))

(defn open [dir]
  (reset! journal (.open (JournalBuilder/of (io/file dir)))))

(defn close []
  (when-let [jrn @journal]
    (.close jrn)))

(defn write [entry]
  (if-let [jrn @journal]
    (.write jrn (.getBytes (encode entry)) Journal$WriteType/SYNC)
    (throw (Exception. "Journal not initialized"))))


(defn jreduce [f acc]
  (if-let [jrn @journal]
    (reduce
     (fn [acc loc]
       (f acc (decode (String. (.read jrn loc Journal$ReadType/SYNC)))))
     acc
     (.redo jrn))
    (throw (Exception. "Journal not initialized"))))

(defn jmap [f]
  (jreduce (fn [acc x] (conj acc (f x))) []))

(comment

  (require '[clojure.java.shell :as sh])

  (sh/sh "rm" "-rf" "/tmp/jrn")

  (sh/sh "mkdir" "/tmp/jrn")

  (open "/tmp/jrn")

  (write {:var 1})
  (write {:var 2})

  (jmap identity)
  (jreduce merge {})

  )

