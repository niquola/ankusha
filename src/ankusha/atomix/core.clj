(ns ankusha.atomix.core
  (:require [clojure.tools.logging :as log]
            [ankusha.util :refer [xmethods get-private-field get-super-private-field]]
            [clojure.spec :as s]
            [clojure.edn :as edn]
            [ankusha.state :as state :refer [with-node]]
            [clojure.java.shell :as sh])
  (:import (io.atomix Atomix AtomixClient AtomixReplica)
           (io.atomix.catalyst.transport.netty NettyTransport)
           (io.atomix.catalyst.transport Address)
           (io.atomix.copycat.server.storage Storage StorageLevel)
           (java.util Collection UUID)
           (java.net InetAddress)
           (java.util.concurrent CompletableFuture)
           (io.atomix.collections DistributedMap)
           (io.atomix.variables DistributedValue)))

(defn get-replica []
  (state/get-in [:atomix :replica]))

(defn- storage [data-dir]
  (-> (Storage/builder)
      (.withDirectory (str data-dir "/atomix"))
      (.build)))

(defn- addr ^Address [{h :host p :port}] (Address. h p))

(defn- addrs ^Collection [ns] (map addr ns))

(defn- localhost []
  (-> (InetAddress/getLocalHost)
      (.getHostName)))

(defn- replica ^AtomixReplica
  [{data-dir :lcl/data-dir {port :ax/port} :lcl/atomix host :lcl/host :as cfg}]
  (log/info "Init replica at " (str  host ":" port))
  (sh/sh "mkdir" "-p" data-dir)
  (-> (AtomixReplica/builder (addr {:host host :port port}))
      (.withTransport (NettyTransport.))
      (.withStorage (storage data-dir))
      (.build)))

(defn- server [rep]
  (-> (get-private-field rep "server")
      .server))

(defn- client [rep]
  (-> (get-super-private-field rep "client")))

(defn- cluster [rep]
  (-> (server rep) 
      .cluster))

(defn- members [rep]
  (.members (cluster rep)))

(defn- on-change [mem]
  (.onStatusChange
   mem
   (reify java.util.function.Consumer
     (accept [this status]
       (log/info "ATOMIX: member status " (.address mem) " changed to " status))))
  (.onTypeChange
   mem
   (reify java.util.function.Consumer
     (accept [this tp]
       (log/info "ATOMIX: member type " (.address mem) " changed to " tp)))))

(defn- subscribe [rep]
  (doseq [mem (members rep)]
    (on-change mem)))

(defn- clear-listeneres [repl]
  (-> repl
      cluster
      (get-private-field "leaveListeners")
      (get-private-field "listeners")
      .clear))

(defn- on-leave [rep]
  (.onLeave (cluster rep)
            (reify java.util.function.Consumer
              (accept [this mem]
                (log/info "ATOMIX: member leaved " (.address mem))))))

(defn- on-join [rep]
  (.onJoin (cluster rep)
           (reify java.util.function.Consumer
             (accept [this mem]
               (log/info "ATOMIX: member joined " (.address mem))
               (on-change mem)))))

(defn get-lock [repl name]
  (.thenRun (.lock (get-lock repl name))
            (reify Runnable
              (run [this]  (log/info "Locked 1")))))

(defn bootstrap []
  (-> (get-replica) .bootstrap .get))

(defn start [lcfg]
  (log/info "Starting replica" lcfg)
  (let [repl (replica lcfg)]
    (subscribe repl)
    (on-join repl)
    (on-leave repl)
    (state/assoc-in [:atomix :replica] repl)))

(defn join [as]
  (if-let [repl (get-replica)]
    (.join (.join repl (addrs as)))
    (log/info "No current replica")))

(defn leave []
  (if-let [repl (get-replica)]
    (.join (.leave repl))
    (log/info "No current replica")))

(defn shutdown []
  (if-let [repl (get-replica)]
    (try
      (log/info "SHUTDOWN" )
      (.join (.shutdown repl))
         (catch Exception e
           (log/error e)))
    (log/error "No replica")))

(defn leader []
  (when-let [rep (get-replica)]
    (.leader (cluster rep))))

(defn status []
  (when-let [repl (get-replica)]
    (->> (members repl)
         (map (fn [m] [(str (.address m)) (str (.status m))])))))

(defn encode [v]
  (String. (pr-str v)))

(defn decode [s]
  (edn/read-string s))

(defn dmap [map-name]
  (when-let [repl (get-replica)]
    (.join (.getMap repl map-name))))


(defn dmap! [map-name]
  (when-let [m (dmap map-name)]
    (into {}
          (for [x (.join (.entrySet m))]
            [(.getKey x) (decode (.getValue x))]))))

(defn dmap-put [map-name key value]
  (when-let [m (dmap map-name)]
    (.join (.put m (name key) (encode value)))))

(defn dmap-get [map-name key]
  (let [m (dmap map-name)]
    (decode (.join (.get m (name key))))))


(defn dset [name]
  (when-let [repl (get-replica)]
    (.join (.getSet repl name))))

(defn dset-put [name value]
  (when-let [m (dset name)]
    (.join (.add m (str value)))))

(defn dvar [var-nm]
  (-> (get-replica)
      (.getValue var-nm)
      .join))

(defn dvar! [var-nm]
  (when-let [val (dvar var-nm)]
    (when-let [ev (.join (.get val))]
      (decode ev))))

(defn dvar-set
  ([var-nm v]
   (when-let [val (dvar var-nm)]
     (.join (.set val (encode v)))))
  ([var-nm v ttl]
    (when-let [val (dvar var-nm)]
      (.join (.set val (encode v) (java.time.Duration/ofSeconds ttl))))))




(comment "rep1"

         (do (sh/sh "rm" "-rf" "/tmp/node-1")
             (sh/sh "rm" "-rf" "/tmp/node-2")
             (sh/sh "rm" "-rf" "/tmp/node-3"))

         (require '[ankusha.config :as conf])

         (with-node "node-1"
           (conf/load-local "sample/node-1.edn")
           (start (conf/local))
           (bootstrap))


         (with-node "node-2"
           (conf/load-local "sample/node-2.edn")
           (start (conf/local))
           (join [{:host "127.0.0.1" :port 4444}]))

         (with-node "node-3"
           (conf/load-local "sample/node-3.edn")
           (start (conf/local))
           (join [{:host "127.0.0.1" :port 4444}]))

         (dvar-set "test" {:a 1} 10)

         (dvar! "test")

         (with-node "node-2"
           (future (log/info "VAL:"
                             (dvar! "test"))))

         (status)
         (leader)
         )
