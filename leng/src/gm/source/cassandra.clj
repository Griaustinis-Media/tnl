(ns gm.source.cassandra
  (:require [universal-db.core :refer [DatabaseAdapter]]
            [qbits.alia :as alia]
            [qbits.hayt :as hayt]))

(defrecord CassandraAdapter [cluster session]
  DatabaseAdapter

  (execute [this qb]
    (let [{:keys [operation columns source conditions joins
                  grouping ordering limit-n offset-n data]} qb]

      (case operation
        :select
        (let [query (cond-> (hayt/select source)
                      ;; Columns
                      (not= columns [:*])
                      (hayt/columns (vec columns))

                      ;; WHERE conditions (must be on partition/clustering keys)
                      (seq conditions)
                      (hayt/where (convert-conditions conditions))

                      ;; ORDER BY (only on clustering columns)
                      (seq ordering)
                      (hayt/order-by (convert-ordering ordering))

                      ;; LIMIT
                      limit-n
                      (hayt/limit limit-n)

                      ;; PER PARTITION LIMIT (Cassandra-specific)
                      offset-n
                      (hayt/per-partition-limit offset-n))]

          (alia/execute session query))

        :insert
        (alia/execute session
                      (hayt/insert source (hayt/values data)))

        :update
        (alia/execute session
                      (-> (hayt/update source)
                        (hayt/set-columns (:set data))
                        (hayt/where (:where data))))

        :delete
        (alia/execute session
                      (-> (hayt/delete source)
                        (hayt/where (:where data))))))))

(defn- convert-conditions
  "Convert universal conditions to Cassandra WHERE format"
  [conditions]
  (into {}
        (map (fn [[op col val]]
               (case op
                 := [col val]
                 :> [col [> val]]
                 :< [col [< val]]
                 :>= [col [>= val]]
                 :<= [col [<= val]]
                 :in [col [:in val]]
                 [col val]))
             conditions)))

(defn- convert-ordering
  "Convert universal ordering to Cassandra format"
  [ordering]
  (vec (map (fn [[col dir]]
              [col (keyword (name dir))])
            (partition 2 ordering))))

(defn create-adapter
  "Create a Cassandra adapter with cluster config"
  [cluster-config]
  (let [cluster (alia/cluster cluster-config)
        session (alia/connect cluster)]
    (->CassandraAdapter cluster session)))

(defn close-adapter
  "Close Cassandra connections"
  [adapter]
  (alia/shutdown (:session adapter))
  (alia/shutdown (:cluster adapter)))
