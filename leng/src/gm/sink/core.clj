(ns gm.sink.core
  "Core abstractions for data sinks")

(defprotocol SinkAdapter
  "Protocol for database write operations"

  (insert [this table records]
    "Insert one or more records into table/collection")

  (update [this table conditions updates]
    "Update records matching conditions")

  (upsert [this table key-columns records]
    "Insert or update based on key columns")

  (delete [this table conditions]
    "Delete records matching conditions")

  (create-table [this table schema]
    "Create table/collection with given schema")

  (batch-insert [this table records batch-size]
    "Insert records in batches for efficiency")

  (transaction [this operations]
    "Execute multiple operations in a transaction (if supported)")

  (connect [this config]
    "Establish connection to the sink")

  (disconnect [this]
    "Close connection to the sink"))

(defrecord WriteOperation
           [adapter                                         ; Sink adapter instance
            operation                                       ; :insert, :update, :upsert, :delete
            table                                           ; Target table/collection
            records                                         ; Data to write
            conditions                                      ; For update/delete operations
            key-columns                                     ; For upsert operations
            batch-size                                      ; For batch operations
            schema])                                        ; For create-table

(defn write-builder
  "Create a new write operation builder"
  [adapter]
  (map->WriteOperation
   {:adapter adapter
    :operation :insert
    :records []
    :conditions []
    :batch-size 1000}))

(defn into-table
  "Specify target table"
  [wb table]
  (assoc wb :table table))

(defn with-records
  "Add records to write"
  [wb records]
  (assoc wb :records records))

(defn with-schema
  "Specify schema for create-table"
  [wb schema]
  (assoc wb :schema schema))

(defn with-conditions
  "Add conditions for update/delete"
  [wb conditions]
  (assoc wb :conditions conditions))

(defn with-keys
  "Specify key columns for upsert"
  [wb key-columns]
  (assoc wb :key-columns key-columns))

(defn as-insert
  "Mark operation as insert"
  [wb]
  (assoc wb :operation :insert))

(defn as-upsert
  "Mark operation as upsert"
  [wb]
  (assoc wb :operation :upsert))

(defn as-update
  "Mark operation as update"
  [wb]
  (assoc wb :operation :update))

(defn as-delete
  "Mark operation as delete"
  [wb]
  (assoc wb :operation :delete))

(defn execute!
  "Execute the write operation"
  [wb]
  (case (:operation wb)
    :insert (insert (:adapter wb) (:table wb) (:records wb))
    :upsert (upsert (:adapter wb) (:table wb) (:key-columns wb) (:records wb))
    :update (update (:adapter wb) (:table wb) (:conditions wb) (:records wb))
    :delete (delete (:adapter wb) (:table wb) (:conditions wb))))

(defmacro w->
  "Write operation threading macro"
  [adapter & forms]
  `(-> (write-builder ~adapter)
     ~@forms))