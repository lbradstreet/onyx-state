;; returns vector of log entries
(defmulti produce-log-entries (fn [task-name state segment] [(type state) task-name]))

;; applies these log entries in turn to the database (whatever form it's in)
(defmulti apply-log-entry (fn [task-name state entry] [(type state) task-name]))

;; produce new segments for immediate egress to next tasks
(defmulti produce-segments (fn [task-name state segment entries] [(type state) task-name]))

;; store these entries in kafka or similar
(defmulti store-entries (fn [producer entries] (type producer))
