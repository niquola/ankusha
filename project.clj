(defproject node_test "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0-alpha8"]
                 [route-map "0.0.3"]
                 [cheshire "5.5.0"]
                 [org.postgresql/postgresql "9.4.1208.jre7"]
                 [org.clojure/java.jdbc "0.5.0"]
                 [org.clojure/core.async "0.2.385"]
                 [http-kit "2.1.19"]
                 [io.atomix/atomix-all "1.0.0-rc9"]
                 [io.atomix.catalyst/catalyst-netty "1.1.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [ch.qos.logback/logback-classic "1.1.7"]
                 [com.github.sbtourist/journalio "1.4.2"]
                 [honeysql "0.7.0"]]

  :repositories [["sonatype-nexus-snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"}]]

  :source-paths ["src"]

  :main ankusha.core

  :uberjar-name "ankusha.jar"

  :profiles {:uberjar {:aot :all :omit-source true}
             :dev {:dependencies []
                   :plugins [[lein-ancient "0.6.10"]]}})
