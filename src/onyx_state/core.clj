(ns onyx-state.core
  (:require [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx-state.state-extensions :as state-extensions]
            [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]))

(defn apply-seen-id 
  "Update the buckets with a new id. 
   Currently only updates the first set and does not update any of the bloom filters."
  [seen-buckets id]
  (update-in seen-buckets [:sets 0] conj id))

(defn seen? 
  "Determine whether an id has been seen before. First check the bloom filter buckets, if
  a bloom filter thinks it has been seen before, check the corresponding set bucket.
  An optimisation is to let the sets expire before the bloom filters do, so we
  get most of the benefit without full memory usage. Currently this
  function just checks the sets."
  [{:keys [blooms sets] :as buckets} id]
  ;; do some pass on bloom-buckets, if any are maybe seen, then check corresponding id-bucket
  (boolean 
    (first 
      (filter (fn [set-bucket]
                (set-bucket id)) 
              sets))))

(defn peer-log-id 
  [event]
  (let [replica (:onyx.core/replica event)
        job-id (:onyx.core/job-id event)
        peer-id (:onyx.core/id event)
        task-id (:onyx.core/task-id event)] 
    (get-in @replica [:task-slot-ids job-id task-id peer-id])))

(defmethod state-extensions/initialise-log :atom [_ event] 
  (get @(:state/entries-log event) (peer-log-id event)))

(defmethod state-extensions/store-log-entries clojure.lang.Atom [log _ entries]
  (swap! log into entries))

(defmethod state-extensions/playback-log-entries clojure.lang.Atom [log _ state apply-fn]
  (reduce apply-fn state @log))

(defmethod state-extensions/store-seen-ids clojure.lang.Atom [log _ seen-ids]
  (swap! log into seen-ids))

(defmethod state-extensions/playback-seen-ids clojure.lang.Atom [seen-log _ bucket-state apply-fn]
  (reduce apply-fn bucket-state @seen-log))

(def state-fn-calls
  ;; allocate log-id (will be group-id in the future) and initialise state and seen buckets from logs
  {:lifecycle/before-task-start (fn inject-state [{:keys [onyx.core/replica 
                                                          onyx.core/state
                                                          state/fns
                                                          state/seen-log
                                                          state/entries-log] 
                                                   :as event} 
                                                  lifecycle]
                                  (let [log-id (peer-log-id event)
                                        _ (info "LOG " log-id " taskid " (:onyx.core/task-id event))
                                        {:keys [produce-log-entries 
                                                apply-log-entry 
                                                produce-segments]} fns
                                        peer-seen-log (get seen-log log-id)
                                        _ (info "INITIALISING")
                                        log (state-extensions/initialise-log :bookkeeper event)
                                        _ (info "INITIALISED")
                                        state (state-extensions/playback-log-entries log
                                                                                     event 
                                                                                     {}
                                                                                     apply-log-entry)
                                        _ (info "PLAYED BACK")
                                        initial-buckets {:blooms [] :sets [#{}]}
                                        _ (info "Doing seen")
                                        seen-buckets (state-extensions/playback-seen-ids peer-seen-log event initial-buckets apply-seen-id)] 
                                    (info "After replay: " state)
                                    {:state/log-id log-id
                                     :state/log log
                                     :state/state (atom state)
                                     :state/seen-buckets (atom seen-buckets)}))
   ;; generate new state log entries, apply them to state, and generate new segments
   ;; only do so if the message has not been seen before
   :lifecycle/after-batch 
   (fn apply-operations [{:keys [state/fns 
                                 state/log-id 
                                 state/log 
                                 state/seen-log 
                                 state/seen-buckets 
                                 state/state] :as event} 
                         lifecycle]
     (let [start-time (System/currentTimeMillis)
           _ (info "BATCH TIME: " start-time)
           {:keys [produce-log-entries apply-log-entry produce-segments id]} fns
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
       ;; ensure these are stored in a single transaction to Kafka
       (state-extensions/store-log-entries log event log-entries)
       (state-extensions/store-seen-ids (seen-log log-id) event seen-ids)
       (info "AFTER BATCH FULL: " (- (System/currentTimeMillis) start-time ))
       ;; segments' should be sent on to next task, 
       ;; but this is too hard to do with proper acking
       {}))})
