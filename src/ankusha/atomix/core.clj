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

(s/fdef replica :args :cfg/local-config)

(defn- replica ^AtomixReplica
  [{data-dir :cfg/data-dir {port :ax/port host :ax/host} :cfg/atomix :as cfg}]
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

(defn start [cfg]
  (log/info "Starting replica" cfg)
  (let [repl (replica cfg)]
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

(defn dvar-set [var-nm v]
  (when-let [val (dvar var-nm)]
    (.join (.set val (encode v)))))

(defn clean-up []
  (sh/sh "rm" "-rf" "/tmp/node-1")
  (sh/sh "rm" "-rf" "/tmp/node-2")
  (sh/sh "rm" "-rf" "/tmp/node-3"))

(comment "rep1"

         (clean-up)

         (with-node "node-1"
           (future
             (println "START NODE-1"
                      (start {:atomix-port 4444
                              :name "node-1"
                              :data-dir "/tmp/node-1"}))
             (println "BOOSTAP NODE-1"
                      (bootstrap))))

         (shutdown)


         (dvar-set "master" {:test 1})

         (dvar! "master")

         (dset "myset")
         (dset-put "myset" "myval")
         (dmap-put "nodes" "node-2" {:a 1})


         (dmap! "nodes")

         (with-node "node-2"
           (dmap! "nodes"))

         (with-node "node-3"
           (dmap! "nodes"))

         (dmap-put "nodes" "n1" {:test 1})

         (dmap-get "nodes" "n1")


         (with-node "node-3"
           (dmap-get "nodes" "n1"))

         (with-node "node-2"
           (future
             (start {:atomix-port 4445
                     :name "node-2"
                     :data-dir "/tmp/node-2"})
             (join [{:host "localhost" :port 4444}])))

         (with-node "node-2"
           (future
             (start {:atomix-port 4445
                     :name "node-2"
                     :data-dir "/tmp/node-2"})
             (bootstrap)))


         (with-node "node-3"
           (start {:atomix-port 4446
                   :name "node-3"
                   :data-dir "/tmp/node-3"})
           (join [{:host "localhost" :port 4444}]))

         (with-node "node-3"
           (start {:atomix-port 4446
                   :name "node-3"
                   :data-dir "/tmp/node-3"})
           (bootstrap))

         (status)
         (leader)
         )