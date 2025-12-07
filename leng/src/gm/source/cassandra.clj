(ns gm.source.cassandra
  (:require
   [clojure.tools.logging :as log]
   [gm.source.core :as core]
   [qbits.alia :as alia]
   [qbits.hayt :as hayt]))

(defrecord CassandraAdapter [session])

(defn- build-contact-points
  "Build contact points with port if specified"
  [config]
  (let [hosts (:contact-points config)
        port (or (:port config) 9042)]
    (mapv #(str % ":" port) hosts)))

(defn- build-session-config
  "Build session configuration with optional authentication.

        For Alia 5.0.0:
        - :contact-points should be [\"host:port\"] format
        - :load-balancing-local-datacenter is required when using contact points
        - Authentication requires:
          * :auth-provider-class (use PlainTextAuthProvider)
          * :auth-provider-user-name
          * :auth-provider-password
        - :session-keyspace sets the keyspace"
  [config]
  (let [base-config {:contact-points (build-contact-points config)
                          ;; Required when using explicit contact points
                     :load-balancing-local-datacenter (or (:datacenter config) "datacenter1")}]
    (cond-> base-config
                    ;; Add authentication if provided
      (and (:username config) (:password config))
      (assoc :auth-provider-class "PlainTextAuthProvider"
             :auth-provider-user-name (:username config)
             :auth-provider-password (:password config))

                    ;; Add keyspace if provided
      (:keyspace config)
      (assoc :session-keyspace (:keyspace config)))))

(defn- simplify-or-condition
  "Cassandra doesn't support OR in WHERE clauses.
             Simplify composite watermark conditions to a simple >= check.
             The in-memory filter will handle deduplication."
  [conditions]
  (cond
         ;; If it's an OR condition with timestamp comparisons, simplify it
    (and (map? conditions) (contains? conditions :or))
    (let [or-clauses (:or conditions)
               ;; Try to find a simple timestamp comparison we can use
          simple-condition (first (filter map? or-clauses))]
      (if simple-condition
                ;; Extract the timestamp column and use >= instead of >
        (let [[[col [op val]]] (seq simple-condition)]
          (if (= op :>)
            {col [:>= val]}  ; Change > to >= to include boundary records
            simple-condition))
        conditions))

         ;; AND conditions might contain OR, recurse
    (and (map? conditions) (contains? conditions :and))
    (update conditions :and #(mapv simplify-or-condition %))

         ;; Regular condition, pass through
    :else
    conditions))

(defn- normalize-query-spec
  "Normalize query spec to handle both old and new parameter names"
  [query-spec]
  (let [;; Handle both :where and :conditions
        where (or (:where query-spec) (:conditions query-spec))
             ;; Simplify OR conditions for Cassandra
        simplified-where (when where (simplify-or-condition where))
             ;; Handle both :limit and :limit-n
        limit (or (:limit query-spec) (:limit-n query-spec))
             ;; Handle both :order-by and :ordering
        order-by (or (:order-by query-spec) (:ordering query-spec))]
    (assoc query-spec
           :where simplified-where
           :limit limit
           :order-by order-by)))

(defn- to-cassandra-value
  "Convert Clojure value to Cassandra-compatible type"
  [val]
  (cond
    (keyword? val) (name val)
    (instance? java.time.Instant val) val
    (instance? java.util.Date val) val
    (instance? java.util.UUID val) val
    (set? val) (vec val)
    (vector? val) val
    (seq? val) (vec val)
    :else val))

(defn- build-where-clause
  "Build CQL WHERE clause from condition map.
        Supports simple conditions, IN/NOT IN, and composite OR/AND conditions."
  [conditions]
  (cond
         ;; Empty conditions
    (empty? conditions)
    {:clause "" :values []}

         ;; OR condition: {:or [{...} {...}]}
    (contains? conditions :or)
    (let [or-results (map build-where-clause (:or conditions))
          or-clauses (map :clause or-results)
          all-values (vec (mapcat :values or-results))]
      {:clause (str "(" (clojure.string/join " OR " or-clauses) ")")
       :values all-values})

         ;; AND condition: {:and [{...} {...}]}
    (contains? conditions :and)
    (let [and-results (map build-where-clause (:and conditions))
          and-clauses (map :clause and-results)
          all-values (vec (mapcat :values and-results))]
      {:clause (str "(" (clojure.string/join " AND " and-clauses) ")")
       :values all-values})

         ;; Regular map of conditions
    :else
    (let [clauses (for [[col val] conditions]
                    (cond
                              ;; IN operator: {:col [:in [values]]} or {:col [:in #{values}]}
                      (and (vector? val) (= :in (first val)))
                      (let [raw-values (second val)
                                    ;; Ensure we have a vector of values
                            values-vec (cond
                                         (set? raw-values) (vec raw-values)
                                         (vector? raw-values) raw-values
                                         (seq? raw-values) (vec raw-values)
                                         :else [raw-values])
                                    ;; Convert each individual value
                            values (mapv to-cassandra-value values-vec)
                            placeholders (clojure.string/join ", " (repeat (count values) "?"))]
                        {:clause (str (name col) " IN (" placeholders ")")
                         :values values})

                              ;; NOT IN operator: {:col [:not-in [values]]} or {:col [:not-in #{values}]}
                      (and (vector? val) (= :not-in (first val)))
                      (let [raw-values (second val)
                                    ;; Ensure we have a vector of values
                            values-vec (cond
                                         (set? raw-values) (vec raw-values)
                                         (vector? raw-values) raw-values
                                         (seq? raw-values) (vec raw-values)
                                         :else [raw-values])
                                    ;; Convert each individual value
                            values (mapv to-cassandra-value values-vec)
                            placeholders (clojure.string/join ", " (repeat (count values) "?"))]
                        {:clause (str (name col) " NOT IN (" placeholders ")")
                         :values values})

                              ;; Comparison operators with single values
                      (and (vector? val) (keyword? (first val)))
                      (let [op (first val)
                            single-val (second val)
                            op-str (case op
                                     :> ">"
                                     :>= ">="
                                     :< "<"
                                     :<= "<="
                                     := "="
                                     :!= "!="
                                     (name op))]
                        {:clause (str (name col) " " op-str " ?")
                         :values [(to-cassandra-value single-val)]})

                              ;; Equality: {:col value}
                      :else
                      {:clause (str (name col) " = ?")
                       :values [(to-cassandra-value val)]}))]
      {:clause (clojure.string/join " AND " (map :clause clauses))
       :values (vec (mapcat :values clauses))})))

(defn fetch
  "Fetch records from Cassandra based on query specification.
       Note: Cassandra doesn't support OR conditions, so composite watermark
       conditions are simplified to use >= and rely on in-memory filtering."
  [adapter query-spec]
  (let [normalized (normalize-query-spec query-spec)
        _ (log/info "Query spec after normalization:" normalized)
        {:keys [table columns where order-by limit allow-filtering]} normalized
        cols-str (if (empty? columns)
                   "*"
                   (clojure.string/join ", " (map name columns)))

        ;; Build WHERE clause
        where-result (if where
                       (build-where-clause where)
                       {:clause "" :values []})
        where-clause (when-not (empty? (:clause where-result))
                       (str " WHERE " (:clause where-result)))

        ;; Build ORDER BY clause
        order-by-clause (when (seq order-by)
                          (str " ORDER BY "
                               (clojure.string/join ", "
                                                    (for [[col direction] order-by]
                                                      (str (name col) " " (name direction))))))

        ;; Build LIMIT clause
        limit-clause (when limit
                       (str " LIMIT " limit))

        ;; Build full CQL query
        cql (str "SELECT " cols-str " FROM " (name table)
                 where-clause
                 order-by-clause
                 limit-clause
                 (when allow-filtering " ALLOW FILTERING"))

            ;; Execute query
        _ (log/info "Executing CQL:" cql)
        _ (log/debug "With values:" (:values where-result))

        results (if (empty? (:values where-result))
                  (alia/execute (:session adapter) cql)
                  (alia/execute (:session adapter) cql {:values (:values where-result)}))]

    (log/info "Fetched" (count results) "records")
    (vec results)))

(defn fetch-lazy
  "Fetch data lazily for streaming"
  [adapter query-spec]
      ;; fetch now handles normalization internally
  (fetch adapter query-spec))

(defn connect
  "Connect to Cassandra"
  [adapter config]
  (let [session-config (build-session-config config)
        session (alia/session session-config)]
    (assoc adapter :session session)))

(defn disconnect
  "Disconnect from Cassandra"
  [adapter]
  (when-let [session (:session adapter)]
    (alia/close session))
  adapter)

(defn create-adapter
  "Create a Cassandra source adapter"
  []
  (map->CassandraAdapter {}))

;; Query spec helpers
(defn query-spec
  "Create a query specification"
  [table]
  {:table table
   :columns [:*]
   :conditions {}
   :ordering []})

(defn where
  "Add WHERE conditions to query"
  [query-spec conditions]
  (if (:where query-spec)
    ;; Merge with existing conditions (combine with AND)
    (update query-spec :where
            (fn [existing]
              (if (and existing (seq existing))
                {:and [existing conditions]}
                conditions)))
    (assoc query-spec :where conditions)))

(defn limit
  "Limit results"
  [spec n]
  (assoc spec :limit-n n))

(defn order-by
  "Order results"
  [spec ordering]
  (assoc spec :ordering ordering))

(defn select-columns
  "Specify columns to select"
  [spec columns]
  (assoc spec :columns columns))

(defn allow-filtering
  "Enable ALLOW FILTERING for the query"
  [spec]
  (assoc spec :allow-filtering true))