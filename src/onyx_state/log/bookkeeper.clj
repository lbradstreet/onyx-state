(ns onyx-state.log.bookkeeper
  (:require [onyx.log.curator :as curator]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx-state.state-extensions :as state-extensions]
            [onyx.extensions :as extensions]
            [com.stuartsierra.component :as component]
            [onyx.static.default-vals :refer [defaults]]
            [clojure.core.async :refer  [chan go >! <! <!! >!! close!]]
            [onyx.log.replica]
            [taoensso.nippy :as nippy]
            [onyx.log.zookeeper :as zk]
            [onyx.api])
  (:import [org.apache.bookkeeper.client LedgerHandle BookKeeper BookKeeper$DigestType]
           [org.apache.bookkeeper.conf ClientConfiguration]
           [org.apache.curator.framework.recipes.leader LeaderSelector LeaderSelectorListenerAdapter]
           [org.apache.curator.framework CuratorFramework CuratorFrameworkFactory]))

(defmethod extensions/apply-log-entry :bookkeeper/book-id
  [{:keys [args]} replica]
  (update-in replica 
             [:bookkeeper-ids (:job-id args) (:task-id args) (:slot-id args)]
             (fn [logs]
               (conj (vec logs) (:ledger-id args)))))

(defmethod extensions/replica-diff :bookkeeper/book-id
  [entry old new]
  {})

(defmethod extensions/reactions :bookkeeper/book-id
  [{:keys [args]} old new diff state]
  [])

(defmethod extensions/fire-side-effects! :bookkeeper/book-id
  [{:keys [args]} old new diff state]
  state)

(defn open-ledger ^LedgerHandle [client id digest-type password]
  (.openLedger client id digest-type password))

(defn create-ledger ^LedgerHandle [client ensemble-size quorum-size digest-type password]
  (.createLedger client ensemble-size quorum-size digest-type password))

(defn bookkeeper
  ([opts]
   (bookkeeper (:zookeeper/address opts)
               (zk/ledgers-path (:onyx/id opts))
               60000))
  ([zk-addr zk-root-path timeout]
   (let [conf (doto (ClientConfiguration.)
                (.setZkServers zk-addr)
                (.setZkTimeout timeout)
                (.setZkLedgersRootPath zk-root-path))]
     (BookKeeper. conf))))

(def logs (atom {}))

(def password 
  (.getBytes "somepass"))

(def digest-type 
  (BookKeeper$DigestType/MAC))

(defn peer-slot-id 
  [event]
  (let [replica (:onyx.core/replica event)
        job-id (:onyx.core/job-id event)
        peer-id (:onyx.core/id event)
        task-id (:onyx.core/task-id event)] 
    (get-in @replica [:task-slot-ids job-id task-id peer-id])))

;; TODO: add zk-timeout for bookkeeper
(defmethod state-extensions/initialise-log :bookkeeper [log-type event] 
  (let [replica (:onyx.core/replica event)
        bk-client (bookkeeper (:onyx.core/peer-opts event))
        ensemble-size 3 ; get from task-map?
        quorum-size 2 ; get from task-map?
        ledger (create-ledger bk-client ensemble-size quorum-size digest-type password)] 
    (>!! (:onyx.core/outbox-ch event)
         {:fn :bookkeeper/book-id
          :args {:job-id (:onyx.core/job-id event)
                 :task-id (:onyx.core/task-id event)
                 :slot-id (peer-slot-id event)
                 :ledger-id (.getId ledger)}})
    ;; TODO: spin until allocated or job/task/peer killed here.
    ledger))

(defmethod state-extensions/playback-log-entries org.apache.bookkeeper.client.LedgerHandle 
  [log event state apply-fn]
  (let [replica @(:onyx.core/replica event)
        slot-id (peer-slot-id event)
        job-id (:onyx.core/job-id event) 
        task-id (:onyx.core/task-id event)
        ledger-ids (get-in replica [:bookkeeper-ids job-id task-id slot-id])
        bk-client (bookkeeper (:onyx.core/peer-opts event))]
    (info "Playing back ledgers for" job-id task-id slot-id " ledger-ids " ledger-ids)
    (reduce (fn [st ledger-id]
              ;; TODO: Do I need to deal with recovery exception in here?
              (let [lh (open-ledger bk-client ledger-id digest-type password)]
                (try
                  (let [last-confirmed (.getLastAddConfirmed lh)
                        _ (info "Opened ledger:" ledger-id "last confirmed:" last-confirmed)]
                    (if (pos? last-confirmed)
                      (let [entries (.readEntries lh 0 last-confirmed)] 
                        (if (.hasMoreElements entries)
                          (loop [st-loop st element (.nextElement entries)]
                            (let [entry-val (nippy/thaw (.getEntry element))
                                  st-loop* (apply-fn st-loop entry-val)]
                              (if (.hasMoreElements entries) 
                                (recur st-loop* (.nextElement entries))
                                st-loop*)))
                          st))  
                      st))
                  (finally
                    (.close lh)))))
            state
            ledger-ids)))

(defmethod state-extensions/close-log org.apache.bookkeeper.client.LedgerHandle
  [log] 
  (.close ^LedgerHandle log))

(defmethod state-extensions/store-log-entries org.apache.bookkeeper.client.LedgerHandle 
  [log event entries]
  (let [start-time (System/currentTimeMillis)]
    (doseq [entry entries] 
      (info "Wrote " entry)
      (.addEntry ^LedgerHandle log (nippy/freeze entry {})))
    (info "TOOK: " (- (System/currentTimeMillis) start-time))
    
    )
  ;; TODO: write out batch end (don't apply until batch end found in others)
  )

; (def handle 
;   (let [ch (chan 100)
;         lh (state-extensions/initialise-log :bookkeeper {:onyx.core/job-id "job-id"
;                                                          :onyx.core/task-id "task-id"
;                                                          :onyx.core/outbox-ch ch})
        
;         output-entry (<!! ch)] 
;     {:handle lh
;      :replica (extensions/apply-log-entry output-entry onyx.log.replica/base-replica)}))

;(identity handle)
;(.close handle)
;(.close bk-client)
;(.isClosed handle)
;(state-extensions/close-log handle)
;(.addEntry handle (nippy/freeze "hithere" {}))

; (defmethod state-extensions/store-seen-ids clojure.lang.Atom [_ _ log seen-ids]
;   (swap! log into seen-ids))

; (defmethod state-extensions/playback-seen-ids clojure.lang.Atom [seen-log event bucket-state apply-fn]
;   (reduce apply-fn bucket-state @seen-log))

; (defrecord LeaderElection [client election-path take-leader-fn leader-selector]
;   component/Lifecycle
;   (component/start [component]
;     (let [leader-selector (doto (LeaderSelector. client 
;                                                  election-path 
;                                                  (proxy [LeaderSelectorListenerAdapter] []
;                                                    (takeLeadership [client]
;                                                      (info "Leader election: taking leadership.")
;                                                      (take-leader-fn))))
;                             (.start))]
;       (assoc component :leader-selector leader-selector)))
;   (component/stop [component]
;     (.stop (:leader-selector component))
;     (assoc component :leader-selector nil)))

; (def leader-selector 
;   (component/start (map->LeaderElection {:client (curator/connect zk-addr)
;                                          :election-path election-path
;                                          :take-leader-fn (fn [])})))

