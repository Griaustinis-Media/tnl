(ns gm.sink.csv
  "CSV file sink adapter"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log])
  (:import (java.io File)))

(defrecord CsvSinkAdapter [output-dir delimiter append?])

(defn create-adapter
  "Create a CSV sink adapter"
  [config]
  (->CsvSinkAdapter
   (:output-dir config "./output")
   (:delimiter config ",")
   (:append? config false)))

(defn connect
  "Connect to CSV output (ensure directory exists)"
  [adapter config]
  (let [output-dir (:output-dir config "./output")
        delimiter (or (:delimiter config) ",")
        append? (get config :append? false)
        dir (io/file output-dir)]

    (when-not (.exists dir)
      (.mkdirs dir)
      (log/info "Created output directory:" output-dir))

    (when-not (.isDirectory dir)
      (throw (ex-info "Output path is not a directory" {:path output-dir})))

    (log/info "Connected to CSV output:" output-dir)
    (assoc adapter
           :output-dir output-dir
           :delimiter delimiter
           :append? append?)))

(defn disconnect
  "Disconnect from CSV (no-op)"
  [adapter]
  (log/info "Disconnected from CSV output")
  adapter)

(defn reserved-columns
  "CSV has no reserved columns"
  [adapter]
  #{})

(defn- write-csv-file
  "Write records to CSV file"
  [file-path records delimiter append?]
  (let [file (io/file file-path)
        file-exists? (.exists file)
        headers (when (seq records) (keys (first records)))
        write-headers? (and (not append?) (or (not file-exists?) (zero? (.length file))))]

    (with-open [writer (io/writer file :append append?)]
      (let [csv-writer (csv/write-csv writer
                                      (cond-> []
                                        write-headers? (conj (map name headers))
                                        true (into (map (fn [record]
                                                          (map #(get record %) headers))
                                                        records)))
                                      :separator (first delimiter))]
        (log/info "Wrote" (count records) "records to" file-path
                  (when append? "(append mode)"))))))

(defn insert
  "Insert records into CSV file"
  [adapter table records]
  (when (seq records)
    (let [file-path (str (:output-dir adapter) "/" (name table) ".csv")
          delimiter (:delimiter adapter)
          append? (:append? adapter)]

      (write-csv-file file-path records delimiter append?)

      {:success true
       :records-written (count records)
       :file file-path})))

(defn batch-insert
  "Insert records in batches"
  [adapter table records batch-size]
  (let [batches (partition-all batch-size records)
        file-path (str (:output-dir adapter) "/" (name table) ".csv")
        delimiter (:delimiter adapter)
        append? (:append? adapter)]

    (log/info "Writing" (count records) "records in" (count batches) "batches")

    (doseq [[idx batch] (map-indexed vector batches)]
      (let [append-this-batch? (or append? (pos? idx))]
        (write-csv-file file-path batch delimiter append-this-batch?)))

    {:success true
     :records-written (count records)
     :batches (count batches)
     :file file-path}))

(defn update-records
  "Update not supported for CSV (append-only)"
  [adapter table conditions updates]
  (throw (ex-info "Update operations not supported for CSV sink"
                  {:adapter adapter})))

(defn upsert
  "Upsert not supported for CSV (append-only)"
  [adapter table key-columns records]
  (throw (ex-info "Upsert operations not supported for CSV sink"
                  {:adapter adapter})))

(defn delete
  "Delete not supported for CSV (append-only)"
  [adapter table conditions]
  (throw (ex-info "Delete operations not supported for CSV sink"
                  {:adapter adapter})))

(defn create-table
  "Create table (CSV file with headers)"
  [adapter table schema]
  (let [file-path (str (:output-dir adapter) "/" (name table) ".csv")
        headers (map name (keys schema))]

    (with-open [writer (io/writer file-path)]
      (csv/write-csv writer [headers] :separator (first (:delimiter adapter))))

    (log/info "Created CSV file:" file-path "with headers:" headers)
    {:success true :file file-path}))

(defn transaction
  "Transactions not supported for CSV (operations are atomic per batch)"
  [adapter operations]
  (log/warn "Transactions not supported for CSV, executing operations sequentially")
  (doseq [op operations]
    ((:fn op))))