(ns gm.source.cassandra-test
  (:require [clojure.test :refer :all]
            [gm.source.cassandra :as cassandra]))

(deftest test-build-where-clause
  (testing "Simple equality condition"
    (let [result (#'cassandra/build-where-clause {:status "active"})]
      (is (= "status = ?" (:clause result)))
      (is (= ["active"] (:values result)))))

  (testing "IN condition"
    (let [result (#'cassandra/build-where-clause
                  {:status [:in ["active" "pending"]]})]
      (is (= "status IN (?, ?)" (:clause result)))
      (is (= ["active" "pending"] (:values result)))))

  (testing "NOT IN condition"
    (let [result (#'cassandra/build-where-clause
                  {:status [:not-in ["deleted" "banned"]]})]
      (is (= "status NOT IN (?, ?)" (:clause result)))
      (is (= ["deleted" "banned"] (:values result)))))

  (testing "Greater than condition"
    (let [ts (java.time.Instant/now)
          result (#'cassandra/build-where-clause {:created_at [:> ts]})]
      (is (= "created_at > ?" (:clause result)))
      (is (= [ts] (:values result)))))

  (testing "OR condition (simplified for Cassandra)"
    (let [ts (java.time.Instant/now)
          condition {:or [{:created_at [:> ts]}
                          {:created_at [:= ts]}]}
          simplified (#'cassandra/simplify-or-condition condition)]
                       ;; Should simplify to >= condition
      (is (contains? simplified :created_at))
      (is (= :>= (first (get simplified :created_at))))))

  (testing "Multiple conditions with AND"
    (let [result (#'cassandra/build-where-clause
                  {:status "active"
                   :age [:> 18]})]
      (is (clojure.string/includes? (:clause result) "AND"))
      (is (= 2 (count (:values result)))))))

(deftest test-to-cassandra-value
  (testing "Convert keyword to string"
    (is (= "active" (#'cassandra/to-cassandra-value :active))))

  (testing "Keep Instant as-is"
    (let [ts (java.time.Instant/now)]
      (is (= ts (#'cassandra/to-cassandra-value ts)))))

  (testing "Convert set to vector"
    (is (= ["a" "b" "c"] (#'cassandra/to-cassandra-value #{"a" "b" "c"})))))