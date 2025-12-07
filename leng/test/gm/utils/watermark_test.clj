(ns gm.utils.watermark-test
  (:require [clojure.test :refer :all]
            [gm.utils.watermark :as watermark]
            [clojure.java.io :as io])
  (:import (java.time Instant)))

(def test-watermark-file ".test-watermark.edn")

(defn cleanup-watermark [f]
      (try
        (f)
        (finally
          (watermark/delete-watermark test-watermark-file))))

(use-fixtures :each cleanup-watermark)

(deftest test-save-and-load-watermark
  (testing "Save and load watermark with timestamp and IDs"
    (let [ts (Instant/now)
          ids #{"id1" "id2" "id3"}
          saved (watermark/save-watermark test-watermark-file ts ids {:test true})
          loaded (watermark/load-watermark test-watermark-file)]

      (is (not (nil? loaded)))
      (is (= 3 (count (:processed-ids loaded))))
      (is (contains? (:processed-ids loaded) "id1"))
      (is (= {:test true} (:metadata loaded))))))

(deftest test-find-max-timestamp
  (testing "Find max timestamp and IDs from records"
    (let [ts1 (Instant/parse "2024-01-01T10:00:00Z")
          ts2 (Instant/parse "2024-01-01T11:00:00Z")
          ts3 (Instant/parse "2024-01-01T11:00:00Z")
          records [{:id "1" :ts ts1}
                   {:id "2" :ts ts2}
                   {:id "3" :ts ts3}]
          [max-ts ids] (watermark/find-max-timestamp records :ts :id)]

      (is (= ts2 max-ts))
      (is (= 2 (count ids)))
      (is (contains? ids "2"))
      (is (contains? ids "3")))))

(deftest test-build-incremental-condition
  (testing "Build simple incremental condition"
    (let [wm (watermark/map->Watermark
              {:last-timestamp (Instant/parse "2024-01-01T10:00:00Z")
               :processed-ids #{}})
          condition (watermark/build-incremental-condition wm :ts :id)]

      (is (contains? condition :ts))
      (is (vector? (get condition :ts)))
      (is (= :> (first (get condition :ts))))))  ;; Changed from :>= to :>

  (testing "Build composite condition with processed IDs"
    (let [ts (Instant/parse "2024-01-01T10:00:00Z")
          wm (watermark/map->Watermark
              {:last-timestamp ts
               :processed-ids #{"id1" "id2"}})
          condition (watermark/build-incremental-condition wm :ts :id)]

      (is (contains? condition :or))
                       ;; Verify the structure of the OR condition
      (is (= 2 (count (:or condition))))
                       ;; First clause should be ts > watermark
      (let [first-clause (first (:or condition))]
        (is (contains? first-clause :ts))
        (is (= :> (first (get first-clause :ts)))))
                       ;; Second clause should be AND with ts = watermark and id NOT IN
      (let [second-clause (second (:or condition))]
        (is (contains? second-clause :and))))))

(deftest test-filter-already-processed
  (testing "Filter out already processed records"
    (let [ts (Instant/parse "2024-01-01T10:00:00Z")
          wm (watermark/map->Watermark
              {:last-timestamp ts
               :processed-ids #{"id1" "id2"}})
          records [{:id "id1" :ts ts}
                   {:id "id2" :ts ts}
                   {:id "id3" :ts ts}
                   {:id "id4" :ts (Instant/parse "2024-01-01T11:00:00Z")}]
          filtered (watermark/filter-already-processed records wm :ts :id)]

      (is (= 2 (count filtered)))
      (is (some #(= "id3" (:id %)) filtered))
      (is (some #(= "id4" (:id %)) filtered)))))

(deftest test-watermark-stats
  (testing "Get watermark statistics"
    (let [ts (Instant/parse "2024-01-01T10:00:00Z")
          wm (watermark/map->Watermark
              {:last-timestamp ts
               :processed-ids #{"id1" "id2" "id3"}
               :last-run (Instant/now)
               :metadata {:records 100}})
          stats (watermark/watermark-stats wm)]

      (is (= ts (:last-timestamp stats)))
      (is (= 3 (:processed-ids-count stats)))
      (is (number? (:age-hours stats))))))