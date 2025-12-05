(ns gm.source.core
  "Core code for our Sink adapter")

(defprotocol DatabaseAdapter
  "Protocol for database-specific implementations"

  (select [this columns]
    "Select specific columns or :* for all")

  (from [this source]
    "Specify data source (table, collection, keyspace, etc.)")

  (where [this condition]
    "Filter results with a condition")

  (join [this join-spec]
    "Join with another source (if supported)")

  (group-by [this columns]
    "Group results by columns")

  (order-by [this specs]
    "Order results (column and direction)")

  (limit [this n]
    "Limit number of results")

  (offset [this n]
    "Skip n results")

  (execute [this query-builder]
    "Execute the built query and return results"))

(defrecord QueryBuilder
           [adapter     ; Database adapter instance
            operation   ; :select, :insert, :update, :delete
            columns     ; Columns to select
            source      ; Table/collection name
            conditions  ; WHERE conditions
            joins       ; JOIN specifications
            grouping    ; GROUP BY columns
            ordering    ; ORDER BY specifications
            limit-n     ; LIMIT value
            offset-n    ; OFFSET value
            data])      ; Data for INSERT/UPDATE

(defn query-builder
  "Create a new query builder with the given adapter"
  [adapter]
  (map->QueryBuilder
   {:adapter adapter
    :operation :select
    :columns [:*]
    :conditions []
    :joins []
    :grouping []
    :ordering []
    :data {}}))

(defn select*
  "Select columns from a query builder"
  [qb & columns]
  (assoc qb :columns (vec columns) :operation :select))

(defn from*
  "Specify data source"
  [qb source]
  (assoc qb :source source))

(defn where*
  "Add WHERE condition"
  [qb condition]
  (update qb :conditions conj condition))

(defn join*
  "Add JOIN (if adapter supports it)"
  [qb join-type source on-condition]
  (update qb :joins conj {:type join-type :source source :on on-condition}))

(defn group-by*
  "Group by columns"
  [qb & columns]
  (assoc qb :grouping (vec columns)))

(defn order-by*
  "Order by specifications"
  [qb & specs]
  (assoc qb :ordering (vec specs)))

(defn limit*
  "Limit results"
  [qb n]
  (assoc qb :limit-n n))

(defn offset*
  "Offset results"
  [qb n]
  (assoc qb :offset-n n))

(defn execute!
  "Execute the query"
  [qb]
  (execute (:adapter qb) qb))

(defmacro q->
  "Query threading macro"
  [adapter & forms]
  `(-> (query-builder ~adapter)
     ~@forms))
