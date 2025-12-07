(ns gm.sink.csv
  "CSV file sink adapter"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [gm.sink.core :as sink]
            [clojure.tools.logging :as log]))

(defrecord CsvSinkAdapter [output-dir delimiter append?])

(defn create-adapter
  "Create a CSV sink adapter"
  [config]
  (->CsvSinkAdapter
   (:output-dir config "./output")
   (:delimiter config ",")
   (:append? config false)))

(defn- write-csv-file
  "Write records to CSV file"
  [file-path records delimiter append?]
  (when (seq records)
    (let [file (io/file file-path)
          file-exists? (.exists file)
          file-has-content? (and file-exists? (pos? (.length file)))
                   ;; Get headers from first record, sorted for consistency
          headers (sort (keys (first records)))
                   ;; Write headers if file is new OR empty, regardless of append flag
          write-headers? (not file-has-content?)]

      (with-open [writer (io/writer file :append append?)]
        (csv/write-csv writer
                       (cond-> []
                         write-headers? (conj (map name headers))
                         true (into (map (fn [record]
                                           (map #(str (get record %)) headers))
                                         records)))
                       :separator (first delimiter))
        (log/info "Wrote" (count records) "records to" file-path
                  (when append? "(append mode)")
                  (when write-headers? " with headers"))))))

;; Implement the SinkAdapter protocol
(extend-type CsvSinkAdapter
  sink/SinkAdapter

  (connect [adapter config]
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

  (disconnect [adapter]
    (log/info "Disconnected from CSV output")
    adapter)

  (reserved-columns [adapter]
    #{})

  (insert [adapter table records]
    (when (seq records)
      (let [file-path (str (:output-dir adapter) "/" (name table) ".csv")
            delimiter (:delimiter adapter)
            append? (:append? adapter)]

        (write-csv-file file-path records delimiter append?)

        {:success true
         :records-written (count records)
         :file file-path})))

  (batch-insert [adapter table records batch-size]
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

  (update-records [adapter table conditions updates]
    (throw (ex-info "Update operations not supported for CSV sink"
                    {:adapter adapter})))

  (upsert [adapter table key-columns records]
    (throw (ex-info "Upsert operations not supported for CSV sink"
                    {:adapter adapter})))

  (delete [adapter table conditions]
    (throw (ex-info "Delete operations not supported for CSV sink"
                    {:adapter adapter})))

  (create-table [adapter table schema]
    (let [file-path (str (:output-dir adapter) "/" (name table) ".csv")
          headers (map name (keys schema))]

      (with-open [writer (io/writer file-path)]
        (csv/write-csv writer [headers] :separator (first (:delimiter adapter))))

      (log/info "Created CSV file:" file-path "with headers:" headers)
      {:success true :file file-path}))

  (transaction [adapter operations]
    (log/warn "Transactions not supported for CSV, executing operations sequentially")
    (doseq [op operations]
      ((:fn op)))))