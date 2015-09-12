(ns onyx-state.state-log
  (:require [onyx.extensions :as extensions]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]))

(defn allocate-state-log-id [replica {:keys [job-id task-id peer-id n-peers] :as args}]
  (let [task-allocations (get-in (:allocations replica) [job-id task-id])] 
    (if (some #{peer-id} task-allocations)
      (let [state-allocations (-> replica 
                                  (get-in [:state-log job-id task-id])
                                  (select-keys task-allocations))
            unallocated-ids (remove (set (vals state-allocations)) 
                                    (range n-peers))
            new-state-allocations (assoc state-allocations peer-id (first unallocated-ids))] 
        (do
          (info "should allocate " (vec unallocated-ids))
          (assoc-in replica [:state-log job-id task-id] new-state-allocations)))
      replica)))

(defmethod extensions/apply-log-entry :allocate-state-log-id
  [{:keys [args]} replica]
  (allocate-state-log-id replica args))

(defmethod extensions/replica-diff :allocate-state-log-id
  [entry old new]
  {})

(defmethod extensions/reactions :allocate-state-log-id
  [{:keys [args]} old new diff state]
  [])

(defmethod extensions/fire-side-effects! :allocate-state-log-id
  [{:keys [args]} old new diff state]
  state)
