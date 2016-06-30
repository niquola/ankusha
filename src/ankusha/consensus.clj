(ns ankusha.consensus
  (:require [clojure.tools.logging :as log]
            [taoensso.nippy :as nippy])
  (:import (io.atomix Atomix AtomixClient AtomixReplica)
           (io.atomix.catalyst.transport Address NettyTransport)
           (io.atomix.copycat.server.storage Storage StorageLevel)
           (java.util Collection UUID)
           (java.net InetAddress)
           (java.util.concurrent CompletableFuture)
           (io.atomix.collections DistributedMap)
           (io.atomix.variables DistributedValue)))

(defonce state (atom {}))

(defn get-replica [name]
  (get @state name))

(defn- storage [cfg]
  (-> (Storage/builder)
      (.withDirectory (str (:data-dir cfg) "/.atomix"))
      (.build)))

(defn- addr ^Address [{h :host p :port}] (Address. h p))

(defn- addrs ^Collection [ns] (map addr ns))

(defn- localhost []
  (-> (InetAddress/getLocalHost)
      (.getHostName)))

(defn- replica ^AtomixReplica
  [cfg]
  (-> (AtomixReplica/builder
       (addr {:host (localhost) :port (:atomix-port cfg)}))
      (.withTransport (NettyTransport.))
      (.withStorage (storage cfg))
      (.build)))


(defn- methods [obj]
  (sort
   (map #(.getName %)
        (.getMethods (type obj)))))

(defn- get-private-field [instance field-name]
  (. (doto (first (filter (fn [x] (.. x getName (equals field-name)))
                          (.. instance getClass getDeclaredFields)))
       (.setAccessible true))
     (get instance)))

(defn- cluster [rep]
  (-> (get-private-field rep "server")
      .server
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


(defn- bootstrap [nm]
  (when-let [repl (get-replica nm)]
    (-> repl .bootstrap .get)))

(defn start [cfg]
  (let [repl (replica cfg)]
    (subscribe repl)
    (on-join repl)
    (on-leave repl)
    (swap! state assoc (:name cfg) repl)))

(defn join [nm as]
  (if-let [repl (get-replica nm)]
    (.join repl (addrs as))
    (log/info "No replica for " nm)))

(defn shutdown [nm]
  (if-let [repl (get-replica nm)]
    (log/info "SHUTDOWN" (.shutdown repl))
    (log/info "No replica for " nm)))

(defn leader [nm]
  (when-let [repl (get-replica nm)]
    (.leader (cluster repl))))

(defn status [nm]
  (when-let [repl (get-replica nm)]
    (->> (members repl)
         (map (fn [m] [(str (.address m)) (str (.status m))])))))

(defn encode [v]
  (String. (nippy/freeze v)))

(defn decode [s]
  (nippy/thaw (.getBytes s)))

(decode (encode {:a 1}))


(defn dmap [nm map-name]
  (when-let [repl (get-replica nm)]
    (.join (.getMap repl map-name))))

(defn dmap! [nm map-name]
  (let [m (dmap nm map-name)]
    (reduce (fn [acc k]
              (assoc acc (keyword k) (decode (.join (.get m k)))))
            {} (.join (.keySet m)))))

(defn dmap-put [nm map-name key value]
  (when-let [m (dmap nm map-name)]
    (.join (.put m (name key) (encode value)))))

(defn dmap-get [nm map-name key]
  (let [m (dmap nm map-name)]
    (decode (.join (.get m (name key))))))


(comment "rep1"
         (start {:atomix-port 4444
                 :name "node-1"
                 :data-dir "/tmp/node-1"})

         (bootstrap "node-1")

         (start {:atomix-port 4445
                 :name "node-2"
                 :data-dir "/tmp/node-2"})

         (bootstrap "node-2")


         (shutdown "node-1")

         (join "node-2" [{:host "localhost" :port 4444}])

         (start {:atomix-port 4446
                 :name "node-3"
                 :data-dir "/tmp/node-3"})

         (bootstrap "node-3")

         (join "node-3" [{:host "localhost" :port 4444}
                         {:host "localhost" :port 4445}])



         (members (get-replica "node-1"))

         (status "node-1")
         (status "node-2")
         (status "node-3")

         (shutdown "node-1")
         (shutdown "node-2")
         (shutdown "node-3")

         (leader "node-1")


         (methods (get-replica "node-1"))



         (dmap-put "node-2" "pg-clusters" "node-1" "newone")
         (dmap-put "node-2" "pg-clusters" "node-2" "newone")

         (.join (.keySet (dmap "node-1" "pg-clusters")))

         (dmap-get "node-1" "pg-clusters" "node-1")
         (dmap-get "node-3" "pg-clusters" "node-1")

         (dmap! "node-3" "pg-clusters")


         )






