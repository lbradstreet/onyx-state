(ns onyx-state.state-log
  (:require [onyx.extensions :as extensions]))

(defn allocate-state-id [replica {:keys [job-id task-id peer-id n-peers] :as args}]
  (let [task-allocations (get-in (:allocations replica) [job-id task-id])] 
    (if (some #{peer-id} task-allocations)
      (let [state-allocations (-> replica 
                                  (get-in [:state job-id task-id])
                                  (select-keys task-allocations))
            remaining-partitions (remove (set (vals state-allocations)) (range n-peers))
            new-state-allocations (assoc state-allocations peer-id (first remaining-partitions))] 
        (assoc-in replica [:state job-id task-id] new-state-allocations))
      replica)))

(defmethod extensions/apply-log-entry :allocate-state-id
  [{:keys [args]} replica]
  (allocate-state-id replica args))

(defmethod extensions/replica-diff :allocate-state-id
  [entry old new]
  {})

(defmethod extensions/reactions :allocate-state-id
  [{:keys [args]} old new diff state]
  [])

(defmethod extensions/fire-side-effects! :allocate-state-id
  [{:keys [args]} old new diff state]
  state)
