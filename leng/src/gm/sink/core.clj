(ns gm.sink.core
  "Core abstractions for data sinks")

(defprotocol SinkAdapter
  "Protocol for writing data to sinks"

  (insert [this table records]
    "Insert records into table")

  (update-records [this table conditions updates]    ; RENAMED from update
    "Update records matching conditions")

  (upsert [this table key-columns records]
    "Insert or update based on key columns")

  (delete [this table conditions]
    "Delete records matching conditions")

  (create-table [this table schema]
    "Create table/collection with given schema")

  (batch-insert [this table records batch-size]
    "Insert records in batches")

  (transaction [this operations]
    "Execute operations in a transaction (if supported)")

  (connect [this config]
    "Establish connection to the sink")

  (disconnect [this]
    "Close connection to the sink"))