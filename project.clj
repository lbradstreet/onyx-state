(defproject onyx-state "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories [["jcenter" {:url "http://jcenter.bintray.com"
                             ;; If a repository contains releases only setting
                             ;; :snapshots to false will speed up dependencies.
                             :snapshots false}]]
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [org.apache.curator/curator-recipes "2.9.0"]
                 [com.stuartsierra/component "0.2.3"]
                 [org.apache.bookkeeper/bookkeeper-server "4.3.1"]
                 [clj-kafka "0.3.2" :exclusions [org.apache.zookeeper/zookeeper zookeeper-clj]]
                 [com.baqend/bloom-filter "1.0.7"]
                 [org.onyxplatform/onyx "0.7.4-SNAPSHOT"]])
