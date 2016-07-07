(defproject node_test "0.1.0-SNAPSHOT"
  :dependencies [[org.clojure/clojure "1.9.0-alpha8"]
                 [cheshire "5.6.3"]
                 [ch.qos.logback/logback-classic "1.1.7"]
                 [clj-antlr "0.2.2"]
                 [clj-jwt "0.1.1"]
                 [cljs-http "0.1.41"]
                 [com.cognitect/transit-clj "0.8.285"]
                 [com.github.sbtourist/journalio "1.4.2"]
                 [com.zaxxer/HikariCP "2.4.7"]
                 [environ "1.0.3"]
                 [garden "1.3.2"]
                 [hiccup "1.0.5"]
                 [honeysql "0.7.0"]
                 [http-kit "2.1.19"]
                 [io.atomix/atomix-all "1.0.0-rc9"]
                 [io.atomix.catalyst/catalyst-netty "1.1.1"]
                 [io.forward/yaml "1.0.3"]
                 [org.clojure/core.async "0.2.385"]
                 [org.clojure/java.jdbc "0.6.1"]
                 [org.clojure/tools.logging "0.3.1"]
                 [org.postgresql/postgresql "9.4.1208.jre7"]
                 [ring "1.5.0"]
                 [ring/ring-defaults "0.2.1"]
                 [route-map "0.0.3"]]

  :repositories [["sonatype-nexus-snapshots" {:url "https://oss.sonatype.org/content/repositories/snapshots"}]]

  :plugins [[lein-ancient "0.6.8"]]

  :source-paths ["src" "clj-pg/src" "clj-pg/test"]

  :main ankusha.core

  :uberjar-name "ankusha.jar"

  :profiles {:uberjar {:aot :all :omit-source true}
             :dev {:dependencies []
                   :plugins [[lein-ancient "0.6.10"]]}})
