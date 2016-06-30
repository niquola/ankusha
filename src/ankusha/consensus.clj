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
      (.withDirectory (str (:data-dir cfg) "/atomix.log"))
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

(comment
  (addr {:host  "localhost" :port 4444})

  (def rep-1 (bootstrap 4444 {:name "node-1" :data-dir "/tmp/node-1"}))
  (def rep-2 (replica 4445 {:name "node-2" :data-dir "/tmp/node-2"}))
  (def rep-3 (replica 4446 {:name "node-3" :data-dir "/tmp/node-3"}))

  rep-1
  rep-2

  (.shutdown rep-2)
  (.shutdown rep-1)
  (.shutdown rep-3)

  (.join rep-2
         (addrs [{:host "localhost" :port 4444}]))

  (.join rep-3
         (addrs [{:host "localhost" :port 4444}
                 {:host "localhost" :port 4445}]))

  (methods rep-2)

  )





