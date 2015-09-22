(ns onyx-state.grouping-state-test
  (:require [clojure.core.async :refer [chan >!! <!! close! sliding-buffer]]
            [clojure.test :refer [deftest is]]
            [taoensso.timbre :refer [info error warn trace fatal] :as timbre]
            [onyx-state.log.bookkeeper]
            [onyx.plugin.core-async :refer [take-segments!]]
            [onyx.api]))

; A few problems so far
; 1. Peers on grouping tasks should be allocated a grouping-id, which is the slot
; that the grouping task fits into. This must be done in core 
; (and not as I've done in onyx-state.state-log), because we need the slots to be stable when a peer
; dies and another takes its place, so that new segments are sent to the 
; peer that recovered for that slot
; Get rid of onyx-state.state-log once 1 is done.
; 2. Should possibly have a new message-id that is stable over retries. 
; This id can be used as a seen id (though possibly we should also support seen ids 
; generated from segments)
; 3. Switching on restart-pred-fn and throwing an exception seems to cause all
; peers to be stopped and started again (at least they warm up spin until the
; peer has restarted) and this causes issues when trying to do recovery in this test


;;;;;;;;;;;;;;
;; :onyx/aggregation-fns code
;; In lieu of an onyx/fn an aggregation task
;; implements the following three or four functions (id-fn is not mandatory 
;; if using some form of internal message-id)

;; A log entry generator which assert or retracts keys
(defn balance-produce-log-entries 
  "Generate the log entry that asserts that a key should be updated to a value"
  [state segment]
  (let [k (:key segment)
        v (:value segment)] 
    (list [:assert k (+ (get state k 0) v)])))

;; A function to apply that log entry to update some agg state
(defn balance-apply-log-entry 
  "Apply a log entry to a state. In this test there are no retractions
   but we implement it anyway"
  [state [op k v]]
  (case op
    :assert (assoc state k v)
    :retract (dissoc state k)))

(defn balance-produce-segments 
  "Produce the segments that should be sent on to the egress tasks.
   Currently not used in this test as we have no good way of sending them to the next tasks."
  [state segment [op k v]]
  (list {:key k
         :sum v}))

(defn segment->id 
  "Turn a segment into an id used to determine whether a segment has been seen before"
  [segment]
  (:id segment))

(def state-fns
  {:id segment->id
   :produce-log-entries balance-produce-log-entries
   :apply-log-entry balance-apply-log-entry
   :produce-segments balance-produce-segments})

;; A fake log medium for writing seen ids to
(def ids-log
  {0 (atom []) 1 (atom [])})

;; A fake log medium for writing state updates to
(def entries-log
  {0 (atom []) 1 (atom [])})

;; An atom that can be updated when the job completes
;; to be checked against the results that we expect
(def final-results (atom {}))

(def insert-calls
  {:lifecycle/before-task-start (fn [event lifecycle]
                                  {:state/fns state-fns
                                   :state/seen-log ids-log
                                   :state/entries-log entries-log})
   :lifecycle/after-task-stop (fn [{:keys [state/seen-log state/entries-log 
                                           state/log-id state/state] :as event} 
                                   lifecycle]
                                (swap! final-results merge @state)
                                (info "Final state: " @state)
                                (info "Final ids log " @(seen-log log-id))
                                (info "Final entries log " @(entries-log log-id))
                                {})}) 

(defn restartable? [& args]
  (info "Restartable")
  true)

;;;;;;;;;;;;;;;;;
;; Test code

(def n-messages 400)

(def input-segments
  (map (fn [n]
         {:id n
          :key (rand-int 10)
          :value (rand-int 1000)}) 
       (range n-messages)))

(def segments-with-repeats
  (reduce into [] 
          (map shuffle 
               (repeat 10 input-segments))))

(def crash-count (atom 0))

(def segment-count (atom 0))

(def crash-on-segments #{2 7 20 60 84 89 34 23 100 344})

(defn identity-crash-sometimes [segment]
  (when (crash-on-segments (swap! segment-count inc)) 
    (Thread/sleep 5000)
    (info "CRRAAAASH")
    (throw (Exception. "Crash to restart.")))
  segment)

(def in-chan (chan 100000))

(def out-chan (chan 100000))

(defn inject-in-ch [event lifecycle]
  {:core.async/chan in-chan})

(defn inject-out-ch [event lifecycle]
  {:core.async/chan out-chan})

(def in-calls
  {:lifecycle/before-task-start inject-in-ch})

(def out-calls
  {:lifecycle/before-task-start inject-out-ch})

(deftest state-grouping-test
  (let [id (java.util.UUID/randomUUID)
        config {:env-config
                {:zookeeper/address "127.0.0.1:2188"
                 :zookeeper/server? true
                 :zookeeper.server/port 2188
                 ;:bookkeeper/port 3196
                 :bookkeeper/local-quorum? true
                 :bookkeeper/server? true}

                :peer-config
                {:zookeeper/address "127.0.0.1:2188"
                 :onyx.peer/job-scheduler :onyx.job-scheduler/greedy
                 :onyx.peer/zookeeper-timeout 60000
                 :onyx.messaging.aeron/embedded-driver? true
                 :onyx.messaging/allow-short-circuit? false
                 :onyx.messaging/impl :aeron
                 :onyx.messaging/peer-ports [40199]
                 :onyx.messaging/bind-addr "localhost"}

                :logging {}}

        env-config (assoc (:env-config config) :onyx/id id)
        peer-config (assoc (:peer-config config) :onyx/id id)
        env (onyx.api/start-env env-config)
        peer-group (onyx.api/start-peer-group peer-config)
        batch-size 2
        catalog [{:onyx/name :in
                  :onyx/plugin :onyx.plugin.core-async/input
                  :onyx/type :input
                  :onyx/medium :core.async
                  :onyx/pending-timeout 5000
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Reads segments from a core.async channel"}

                 {:onyx/name :agg
                  ;:onyx/fn :clojure.core/identity
                  :onyx/fn ::identity-crash-sometimes
                  :onyx/type :function
                  :onyx/group-by-key :key
                  :onyx/restart-pred-fn ::restartable?
                  :onyx/min-peers 2
                  :onyx/max-peers 2
                  :onyx/flux-policy :recover ;; should only recover if possible?
                  :onyx/batch-size batch-size}

                 {:onyx/name :out
                  :onyx/plugin :onyx.plugin.core-async/output
                  :onyx/type :output
                  :onyx/medium :core.async
                  :onyx/batch-size batch-size
                  :onyx/max-peers 1
                  :onyx/doc "Writes segments to a core.async channel"}]

        workflow [[:in :agg] [:agg :out]]

        lifecycles [{:lifecycle/task :in
                     :lifecycle/calls ::in-calls}
                    {:lifecycle/task :in
                     :lifecycle/calls :onyx.plugin.core-async/reader-calls}
                    {:lifecycle/task :agg
                     :lifecycle/calls ::insert-calls}
                    {:lifecycle/task :agg
                     :lifecycle/calls :onyx-state.core/state-fn-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls ::out-calls}
                    {:lifecycle/task :out
                     :lifecycle/calls :onyx.plugin.core-async/writer-calls}]

        v-peers (onyx.api/start-peers 6 peer-group)
        _ (Thread/sleep 2000)

        ;; Load up data occurring multiple times, simulating retries etc
        _ (doseq [segment input-segments #_segments-with-repeats]
            (>!! in-chan segment))
        _ (>!! in-chan :done)
        _ (close! in-chan)

        _ (onyx.api/submit-job peer-config
                               {:catalog catalog
                                :workflow workflow
                                :lifecycles lifecycles
                                :task-scheduler :onyx.task-scheduler/balanced})
        results (take-segments! out-chan)]

    ;; Wait, for longer because await-job-completion doesn't wait for task shutdown
    (Thread/sleep 1000)

    (println "Actual is " 
            (into {} 
                 (map (fn [[k v]]
                        (vector k (reduce + (map :value v)))) 
                      (group-by :key input-segments))) 
             )

    (is (= (into {} 
                 (map (fn [[k v]]
                        (vector k (reduce + (map :value v)))) 
                      (group-by :key input-segments)))
           @final-results))

    (doseq [v-peer v-peers]
      (onyx.api/shutdown-peer v-peer))
    (onyx.api/shutdown-peer-group peer-group) 
    (onyx.api/shutdown-env env)))
