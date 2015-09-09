(ns onyx-state.core-test
  (:require [clojure.test :refer :all]
            [onyx-state.core :refer :all]))

(deftest state-updates-segments
  (testing "state updates and entries generation"
    (let [replica nil
          peer-state nil
          log (atom [])
          state {}
          segment {:key "john" :value 99}
          entries (produce-log-entries state segment)
          state' (reduce apply-log-entry state entries)
          segments (mapcat #(produce-segments state' segment %) entries)
          _ (store-entries replica peer-state log entries)]
      (is (= entries [[:assert "john" 99]]))
      (is (= state' {"john" 99}))
      (is (= segments [{:key "john" :sum 99}]))
      (is (= @log [[:assert "john" 99]]))

      (let [segment2 {:key "john" :value 33}
            entries2 (produce-log-entries state' segment2)
            state2 (reduce apply-log-entry state' entries2)
            segments2 (mapcat #(produce-segments state2 segment2 %) entries2)
            _ (store-entries replica peer-state log entries2)]

        (is (= entries2 [[:assert "john" 132]]))
        (is (= state2 {"john" 132}))

        (let [state-after-playback (reduce apply-log-entry 
                                           {} 
                                           (read-entries replica peer-state log))]
          (is (= state-after-playback state2)))))))
