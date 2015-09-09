(ns onyx-state.core
  (:require [com.stuartsierra.component :as component]))

(defn initialise-state [task-map]
  {})

(defn produce-log-entries [state {:keys [key value]}]
  (list [:assert key (+ (get state key 0) value)]))

(defn apply-log-entry [state [op k v]]
  (case op
    :assert (assoc state k v)
    :retract (dissoc state k)))

;; produce-segments may take a number of entries to allow for optimisations to be made
;; by looking at all entries e.g. dedupe? Perhaps s
(defn produce-segments [state segment [op k v]]
  (list {:key k
         :sum v}))

;; alternative to :onyx/fn will point here
(def balance 
  {:before-start initialise-state
   ;; :seen? 
   ;; need some function for whether its been seen before
   ;; and for bloom filter etc
   :produce-log-entries produce-log-entries
   :apply-log-entry apply-log-entry
   :produce-segments produce-segments})


;;;;
;; TODO Add duplicate message tracking here
;; e.g.  bloom filter, plus additional set checks

(defn seen? [message bloom-buckets id-buckets]
  ;; do some pass on bloom-buckets, if any are maybe seen, then check corresponding id-bucket
  )



;;;;
;; For onyx to implement
;; We can implement log storage for Kafka and maybe ZK (if small data is used and we gc)

(defmulti store-entries (fn [replica state log entries]
                          (cond (and (= (type log)
                                        clojure.lang.Atom)
                                     (vector? @log))
                                :vector-atom)))

(defmethod store-entries :vector-atom [_ _ log entries]
  (swap! log into entries))

(defmulti read-entries (fn [replica state log]
                         (cond (and (= (type log)
                                       clojure.lang.Atom)
                                     (vector? @log))
                                :vector-atom)))

(defmethod read-entries :vector-atom [_ _ log]
  ;; should spin here until allocated an id, then read them all
  @log)

;; returns vector of log entries
;(defmulti produce-log-entries (fn [task-name state segment] [(type state) task-name]))

;; applies these log entries in turn to the database (whatever form it's in)
;(defmulti apply-log-entry (fn [task-name state entry] [(type state) task-name]))
;
;; produce new segments for immediate egress to next tasks
;(defmulti produce-segments (fn [task-name state segment entries] [(type state) task-name]))

;; store these entries in kafka or similar
;(defmulti store-entries (fn [producer entries] (type producer))
