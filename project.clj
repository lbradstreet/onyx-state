(defproject onyx-state "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [com.stuartsierra/component "0.2.3"]
                 [clj-kafka "0.3.2" :exclusions [org.apache.zookeeper/zookeeper zookeeper-clj]]
                 [org.onyxplatform/onyx "0.7.3"]])