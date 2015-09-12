(ns onyx-state.core
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]))

(defn apply-seen-id [seen-buckets id]
  (update-in seen-buckets [:sets 0] conj id))

(defn seen? 
  "Determine whether an id has been seen before. First check the bloom filter buckets, if
  a bloom filter thinks it has been seen before, check the corresponding set bucket.
  The idea is that the sets will expire before the buckets do.
  Currently this function just checks the sets."
  [{:keys [blooms sets] :as buckets} id]
  ;; do some pass on bloom-buckets, if any are maybe seen, then check corresponding id-bucket
  (boolean 
    (first 
      (filter (fn [set-bucket]
                (set-bucket id)) 
              sets))))

;;;;
;; For onyx to implement
;; We can implement log storage for Kafka and maybe ZK (if small data is used and we gc)
(defmulti store-log-entries 
  "Store state update [op k v] entries in a log"
  (fn [replica state log entries]
    (cond (and (= (type log)
                  clojure.lang.Atom)
               (vector? @log))
          :vector-atom)))

(defmethod store-log-entries :vector-atom [_ _ log entries]
  (swap! log into entries))

(defmulti read-log-entries 
  "Read state update [op k v] entries from log"
  (fn [replica state log]
    (cond (and (= (type log)
                  clojure.lang.Atom)
               (vector? @log))
          :vector-atom)))

(defmethod read-log-entries :vector-atom [_ _ log]
  @log)

(defmulti store-seen-ids 
  "Store seen ids in a log. Ideally these will be timestamped for auto expiry"
  (fn [replica state seen-log seen-ids]
                          (cond (and (= (type seen-log)
                                        clojure.lang.Atom)
                                     (vector? @seen-log))
                                :vector-atom)))

(defmethod store-seen-ids :vector-atom [_ _ log seen-ids]
  (swap! log into seen-ids))

(defmulti read-seen-ids 
  "Read seen ids from a log"
  (fn [replica state seen-log]
    (cond (and (= (type seen-log)
                  clojure.lang.Atom)
               (vector? @seen-log))
          :vector-atom)))

(defmethod read-seen-ids :vector-atom [_ _ seen-log]
  @seen-log)

(defn allocate-log-id 
  "Allocates a unique id to the peer, such as in the kafka plugin
  This will need to be scrapped as we really need a grouping id
  i.e. slot that will ensure that messages will continue to be sent to the peer that 
  recovers"
  [event]
  (let [replica (:onyx.core/replica event)
        state (:onyx.core/state event)
        job-id (:onyx.core/job-id event)
        peer-id (:onyx.core/id event)
        task-id (:onyx.core/task-id event)
        n-peers (:onyx/max-peers (:onyx.core/task-map event))
        _ (>!! (:onyx.core/outbox-ch event)
               {:fn :allocate-state-log-id
                :args {:n-peers n-peers
                       :job-id job-id
                       :task-id task-id 
                       :peer-id peer-id}})] 
    (loop [state-log-id (get-in @replica [:state-log job-id task-id peer-id])] 
      (if state-log-id 
        state-log-id
        (do
          ;(info "Spinning while waiting to be allocated state log id")
          (Thread/sleep 50)
          (recur (get-in @replica [:state-log job-id task-id peer-id])))))))

(def state-fn-calls
  ;; allocate log-id (will be group-id in the future) and initialise state and seen buckets from logs
  {:lifecycle/before-task-start (fn inject-state [{:keys [onyx.core/replica 
                                                          onyx.core/state
                                                          state/fns
                                                          state/seen-log
                                                          state/entries-log] 
                                                   :as event} 
                                                  lifecycle]
                                  (let [log-id (allocate-log-id event)
                                        {:keys [produce-log-entries apply-log-entry produce-segments]} fns
                                        peer-seen-log (get seen-log log-id)
                                        seen-ids (read-seen-ids replica state peer-seen-log)
                                        peer-entries-log (get entries-log log-id)
                                        read-entries (read-log-entries replica state peer-entries-log)
                                        state (reduce apply-log-entry {} read-entries)
                                        initial-buckets {:blooms [] :sets [#{}]}
                                        seen-buckets (reduce apply-seen-id initial-buckets seen-ids)] 
                                    {:state/log-id log-id
                                     :state/state (atom state)
                                     :state/seen-buckets (atom seen-buckets)}))
   ;; generate new state log entries, apply them to state, and generate new segments
   ;; only do so if the message has not been seen before
   :lifecycle/after-batch 
   (fn apply-operations [{:keys [state/fns 
                                 state/log-id 
                                 state/entries-log 
                                 state/seen-log 
                                 state/seen-buckets 
                                 state/state] :as event} 
                         lifecycle]
     (let [{:keys [produce-log-entries apply-log-entry produce-segments id]} fns
           segments (map :message (mapcat :leaves (:tree (:onyx.core/results event))))
           [state' 
            seen-buckets' 
            log-entries 
            seen-ids 
            segments'] (reduce (fn [[state seen-buckets log-entries seen-ids segments] segment]
                                 (let [seg-id (id segment)] 
                                   (if (seen? seen-buckets seg-id) 
                                     ;; TODO: if we've seen the id again, we should possibly 
                                     ;; add it to a later bucket, as it may mean that retries 
                                     ;; are happening and we shouldn't let it expire
                                     [state seen-buckets log-entries seen-ids segments]
                                     (let [new-log-entries (produce-log-entries state segment)
                                           updated-seen-buckets (apply-seen-id seen-buckets seg-id)
                                           updated-state (reduce (fn [st entry] 
                                                                   (apply-log-entry st entry))
                                                                 state
                                                                 new-log-entries)
                                           new-segments (mapcat (fn [entry] 
                                                                  (produce-segments updated-state segment entry))
                                                                new-log-entries)]

                                       (vector updated-state
                                               updated-seen-buckets
                                               (into log-entries new-log-entries)
                                               (conj seen-ids seg-id)
                                               (into segments new-segments)))))) 
                               [@state @seen-buckets [] [] []]
                               segments)]
       (reset! state state')
       (reset! seen-buckets seen-buckets')
       (let [replica (:onyx.core/replica event)
             state (:onyx.core/state event)] 
         ;; ensure these are stored in a single transaction to Kafka
         (store-log-entries replica state (entries-log log-id) log-entries)
         (store-seen-ids replica state (seen-log log-id) seen-ids))
       ;; segments' should be sent on to next task, 
       ;; but this is too hard to do with proper acking
       {}))})
