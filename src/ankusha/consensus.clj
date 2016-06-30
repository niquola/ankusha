(ns ankusha.consensus
  (:import (io.atomix Atomix AtomixClient AtomixReplica)
           (io.atomix.catalyst.transport Address NettyTransport)
           (io.atomix.copycat.server.storage Storage StorageLevel)
           (java.util Collection UUID)
           (java.net InetAddress)
           (java.util.concurrent CompletableFuture)
           (io.atomix.collections DistributedMap)
           (io.atomix.variables DistributedValue)))

(defn storage [cfg]
  (-> (Storage/builder)
      (.withDirectory (str (:data-dir cfg) "/.atomix"))
      (.build)))

(defn- addr ^Address [{h :host p :port}] (Address. h p))

(defn- addrs ^Collection [ns] (map addr ns))

(defn localhost []
  (-> (InetAddress/getLocalHost)
      (.getHostName)))

(defn replica ^AtomixReplica
  [port cfg]
  (-> (AtomixReplica/builder
       (addr {:host (localhost) :port port}))
      (.withTransport (NettyTransport.))
      (.withStorage (storage cfg))
      (.build)))

(defn bootstrap [port cfg]
  (-> (replica port cfg)
      .bootstrap
      .get))

(defn methods [obj]
  (sort
   (map #(.getName %)
        (.getMethods (type obj)))))

(defn get-private-field [instance field-name]
  (. (doto (first (filter (fn [x] (.. x getName (equals field-name)))
                          (.. instance getClass getDeclaredFields)))
       (.setAccessible true))
     (get instance)))

(defn cluster [rep]
  (-> (get-private-field rep "server")
      .server
      .cluster))

(defn members [rep]
  (.members (cluster rep)))

(defn on-change [mem]
  (.onStatusChange
   mem
   (reify java.util.function.Consumer
     (accept [this status]
       (println "Status of " mem " Changed to " status)))))


(comment "rep1"
         (def rep-1 (bootstrap 4444 {:name "node-1" :data-dir "/tmp/node-1"}))

         (.shutdown rep-1)

         (.type rep-1)

         (members rep-1)

         (def lock-1 (get-lock rep-1 "master"))
         (.thenRun (.lock lock-1)
                   (reify Runnable
                     (run [this]  (println "Locked 1"))))
         )

(comment "rep2"
         (def rep-2 (replica 4445 {:name "node-2" :data-dir "/tmp/node-2"}))

         (.join rep-2
                (addrs [{:host "localhost" :port 4444}]))

         (.type rep-2)

         rep-2

         (methods rep-2)


         (map (fn [m] (println (str (.address m)) " " (str (.status m))))
              (members rep-2))

         (cluster rep-2)

         (defn subscribe [rep]
           (doseq [mem (members rep)]
             (println "MEMBER" mem)
             (on-change mem)))

         (-> rep-2
             cluster
             (get-private-field "leaveListeners")
             (get-private-field "listeners")
             .clear)

         (-> rep-2
             cluster
             (get-private-field "joinListeners")
             (get-private-field "listeners")
             .clear)

         (.onLeave (cluster rep-2)
                   (reify java.util.function.Consumer
                     (accept [this mem]
                       (println "LEAVED " mem))))

         (.onJoin (cluster rep-2)
                  (reify java.util.function.Consumer
                    (accept [this mem]
                      (println "JOINED " mem)
                      (on-change mem))))

         (methods (cluster rep-2))

         (.leader (cluster rep-2))


         (subscribe rep-2)

         (.shutdown rep-2)
         )

(comment "rep3"

         (def rep-3 (replica 4446 {:name "node-3" :data-dir "/tmp/node-3"}))

         (.join rep-3
                (addrs [{:host "localhost" :port 4444}
                        {:host "localhost" :port 4445}]))

         rep-3
         (members rep-3)

         (map (fn [m] (println (str (.address m)) " " (str (.status m))))
              (members rep-3))

         (.type rep-3)

         (.leave rep-3)

         (.shutdown rep-3)

         )






