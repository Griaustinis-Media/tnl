(ns gm.source.csv
  "CSV file source adapter"
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]))

(defrecord CsvAdapter [file-path delimiter])

(defn create-adapter
  "Create a CSV adapter"
  []
  (->CsvAdapter nil ","))

(defn connect
  "Connect to CSV file(s)"
  [adapter config]
  (let [file-path (:file-path config)
        delimiter (or (:delimiter config) ",")]
    (when-not file-path
      (throw (ex-info "CSV file path is required" {:config config})))
    (when-not (.exists (io/file file-path))
      (throw (ex-info "CSV file not found" {:path file-path})))
    (log/info "Connected to CSV file:" file-path)
    (assoc adapter
           :file-path file-path
           :delimiter delimiter)))

(defn disconnect
  "Disconnect from CSV (no-op)"
  [adapter]
  (log/info "Disconnected from CSV")
  adapter)

(defn- parse-csv
  "Parse CSV file into maps"
  [file-path delimiter]
  (with-open [reader (io/reader file-path)]
    (let [data (csv/read-csv reader :separator (first delimiter))
          headers (map keyword (first data))
          rows (rest data)]
      (mapv #(zipmap headers %) rows))))

(defn- apply-where-filter
  "Filter records based on WHERE conditions"
  [records conditions]
  (if (empty? conditions)
    records
    (filter
     (fn [record]
       (every?
        (fn [[col condition]]
          (let [value (get record col)]
            (cond
                            ;; IN operator
              (and (vector? condition) (= :in (first condition)))
              (contains? (set (second condition)) value)

                            ;; NOT IN operator
              (and (vector? condition) (= :not-in (first condition)))
              (not (contains? (set (second condition)) value))

                            ;; Greater than
              (and (vector? condition) (= :> (first condition)))
              (> (compare value (second condition)) 0)

                            ;; Greater than or equal
              (and (vector? condition) (= :>= (first condition)))
              (>= (compare value (second condition)) 0)

                            ;; Less than
              (and (vector? condition) (= :< (first condition)))
              (< (compare value (second condition)) 0)

                            ;; Less than or equal
              (and (vector? condition) (= :<= (first condition)))
              (<= (compare value (second condition)) 0)

                            ;; Equality
              :else
              (= value condition))))
        conditions))
     records)))

(defn- select-columns-from-records
  "Select specific columns from records"
  [records columns]
  (if (or (empty? columns) (some #{:*} columns))
    records
    (mapv #(select-keys % columns) records)))

(defn query-spec
  "Create a query specification for CSV"
  [table]
  {:table table
   :columns []
   :where {}
   :limit nil})

(defn select-columns
  "Add column selection to query"
  [query-spec columns]
  (assoc query-spec :columns columns))

(defn where
  "Add WHERE conditions to query"
  [query-spec conditions]
  (update query-spec :where merge conditions))

(defn allow-filtering
  "No-op for CSV (always allows filtering)"
  [query-spec]
  query-spec)

(defn limit
  "Add LIMIT to query"
  [query-spec n]
  (assoc query-spec :limit n))

(defn offset
  "Add OFFSET to query (for incremental loading)"
  [query-spec n]
  (assoc query-spec :offset n))

(defn fetch
  "Fetch records from CSV file based on query specification"
  [adapter query-spec]
  (let [{:keys [table columns where limit offset]} query-spec
            ;; Use file-path from adapter, not the table name
        file-path (:file-path adapter)

        _ (when-not file-path
            (throw (ex-info "CSV file path not set in adapter" {:adapter adapter})))

            ;; Parse CSV
        all-records (parse-csv file-path (:delimiter adapter))

            ;; Apply OFFSET (skip already processed rows)
        records-after-offset (if offset
                               (drop offset all-records)
                               all-records)

            ;; Apply WHERE filter
        filtered (apply-where-filter records-after-offset where)

            ;; Select columns
        selected (select-columns-from-records filtered columns)

            ;; Apply limit
        limited (if limit
                  (take limit selected)
                  selected)

            ;; Add row number metadata (for watermarking)
        with-row-numbers (map-indexed
                          (fn [idx record]
                            (assoc record :__row_number (+ (or offset 0) idx)))
                          limited)]

    (log/info "Fetched" (count with-row-numbers) "records from CSV"
              (when offset (str "(offset: " offset ")")))
    (vec with-row-numbers)))

(defn fetch-lazy
  "Fetch data lazily for streaming (same as fetch for CSV)"
  [adapter query-spec]
  (fetch adapter query-spec))
