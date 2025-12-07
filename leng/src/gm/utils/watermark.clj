(ns gm.utils.watermark
  "Watermark management for incremental data pipelines"
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]))

(defrecord Watermark [last-timestamp processed-ids last-run metadata])

(defn- timestamp->string
  "Convert any timestamp type to ISO-8601 string for serialization"
  [ts]
  (cond
    (nil? ts) nil
    (string? ts) ts
    (instance? java.time.Instant ts) (.toString ts)
    (instance? java.util.Date ts) (.toString (.toInstant ts))
    (number? ts) (.toString (java.time.Instant/ofEpochMilli ts))
    :else (str ts)))

(defn- string->instant
  "Parse ISO-8601 string back to Instant, or return as-is if already parsed"
  [s]
  (cond
    (nil? s) nil
    (instance? java.time.Instant s) s
    (string? s) (try
                  (java.time.Instant/parse s)
                  (catch Exception e
                    (log/warn "Could not parse timestamp string:" s)
                    s))
    :else s))

(defn load-watermark
  "Load the last processed timestamp from file.
       Returns a Watermark record or nil if no watermark exists."
  [watermark-file]
  (let [file (io/file watermark-file)]
    (if (.exists file)
      (try
        (let [data (edn/read-string (slurp file))
              parsed-data (-> data
                            (update :last-timestamp string->instant)
                            (update :last-run string->instant)
                            (update :processed-ids #(or % #{})))]
          (map->Watermark parsed-data))
        (catch Exception e
          (log/error "Failed to load watermark file:" (.getMessage e))
          nil))
      nil)))

(defn save-watermark
  "Save the watermark to file.
       For timestamp-based: (save-watermark file timestamp ids metadata)
       For row-based: (save-watermark file row-number metadata)"
  ([watermark-file value-or-timestamp metadata-or-ids]
   (if (set? metadata-or-ids)
         ;; Three args, old signature: timestamp, ids, (missing metadata)
     (save-watermark watermark-file value-or-timestamp metadata-or-ids {})
         ;; Two args, row-based: row-number, metadata
     (let [watermark {:last-row-number value-or-timestamp
                      :last-run (timestamp->string (java.time.Instant/now))
                      :metadata metadata-or-ids}]
       (try
         (spit watermark-file (pr-str watermark))
         (log/info "Saved watermark:" {:row-number value-or-timestamp})
         watermark
         (catch Exception e
           (log/error "Failed to save watermark:" (.getMessage e))
           (throw e))))))
  ([watermark-file timestamp processed-ids metadata]
       ;; Four args, timestamp-based
   (let [watermark {:last-timestamp (timestamp->string timestamp)
                    :processed-ids (set processed-ids)
                    :last-run (timestamp->string (java.time.Instant/now))
                    :metadata metadata}]
     (try
       (spit watermark-file (pr-str watermark))
       (log/info "Saved watermark:" {:timestamp (timestamp->string timestamp)
                                     :id-count (count processed-ids)})
       (map->Watermark watermark)
       (catch Exception e
         (log/error "Failed to save watermark:" (.getMessage e))
         (throw e))))))

(defn delete-watermark
  "Delete the watermark file"
  [watermark-file]
  (let [file (io/file watermark-file)]
    (when (.exists file)
      (.delete file)
      (log/info "Deleted watermark file:" watermark-file))))

(defn- compare-timestamps
  "Compare two timestamps, returning -1, 0, or 1"
  [ts1 ts2]
  (cond
    (nil? ts1) -1
    (nil? ts2) 1

    (and (instance? java.time.Instant ts1) (instance? java.time.Instant ts2))
    (.compareTo ts1 ts2)

    (and (instance? java.util.Date ts1) (instance? java.util.Date ts2))
    (.compareTo ts1 ts2)

    (and (number? ts1) (number? ts2))
    (compare ts1 ts2)

    :else
    (let [millis1 (cond
                    (instance? java.time.Instant ts1) (.toEpochMilli ts1)
                    (instance? java.util.Date ts1) (.getTime ts1)
                    (number? ts1) ts1
                    :else 0)
          millis2 (cond
                    (instance? java.time.Instant ts2) (.toEpochMilli ts2)
                    (instance? java.util.Date ts2) (.getTime ts2)
                    (number? ts2) ts2
                    :else 0)]
      (compare millis1 millis2))))

(defn find-max-row-number
  "Find the maximum row number from a collection of records (for CSV sources)"
  [records]
  (when (seq records)
    (apply max (map :__row_number records))))

(defn find-max-timestamp
  "Find the maximum timestamp from a collection of records.
       Returns [max-timestamp ids-at-max-timestamp]"
  [records timestamp-column id-column]
  (when (seq records)
    (let [;; Find the maximum timestamp
          max-ts (reduce (fn [max-so-far record]
                           (let [ts (get record timestamp-column)]
                             (if (or (nil? max-so-far)
                                     (pos? (compare-timestamps ts max-so-far)))
                               ts
                               max-so-far)))
                         nil
                         records)
                  ;; Get all IDs that have this max timestamp
          ids-at-max (into #{}
                           (comp
                            (filter #(zero? (compare-timestamps (get % timestamp-column) max-ts)))
                            (map #(get % id-column)))
                           records)]
      [max-ts ids-at-max])))

(defn find-min-timestamp
  "Find the minimum timestamp from a collection of records."
  [records timestamp-column]
  (when (seq records)
    (let [timestamps (map #(get % timestamp-column) records)]
      (reduce (fn [min-ts ts]
                (cond
                  (nil? min-ts) ts
                  (nil? ts) min-ts

                  (and (instance? java.time.Instant min-ts)
                       (instance? java.time.Instant ts))
                  (if (.isBefore ts min-ts) ts min-ts)

                  (and (instance? java.util.Date min-ts)
                       (instance? java.util.Date ts))
                  (if (.before ts min-ts) ts min-ts)

                  (and (number? min-ts) (number? ts))
                  (min min-ts ts)

                  :else
                  (let [min-millis (cond
                                     (instance? java.time.Instant min-ts)
                                     (.toEpochMilli min-ts)
                                     (instance? java.util.Date min-ts)
                                     (.getTime min-ts)
                                     (number? min-ts)
                                     min-ts
                                     :else
                                     (throw (ex-info "Unsupported timestamp type"
                                                     {:type (type min-ts)})))
                        ts-millis (cond
                                    (instance? java.time.Instant ts)
                                    (.toEpochMilli ts)
                                    (instance? java.util.Date ts)
                                    (.getTime ts)
                                    (number? ts)
                                    ts
                                    :else
                                    (throw (ex-info "Unsupported timestamp type"
                                                    {:type (type ts)})))]
                    (if (< ts-millis min-millis) ts min-ts))))
              nil
              timestamps))))

(defn build-incremental-condition
  "Build a condition map for incremental queries based on watermark.

       For timestamp-based watermarks (databases):
         Uses composite condition: timestamp > watermark OR (timestamp = watermark AND id NOT IN processed-ids)

       For row-based watermarks (CSV files):
         Returns nil (offset is handled separately in CSV adapter)

       Args:
         watermark: Watermark record or nil
         col1: For databases: timestamp-column keyword. For CSV: not used
         col2: For databases: id-column keyword. For CSV: not used

       Returns condition map suitable for query builders, or nil for row-based."
  [watermark timestamp-column id-column]
  (cond
        ;; Timestamp-based watermark (database)
    (and watermark (:last-timestamp watermark))
    (let [ts (:last-timestamp watermark)
          processed-ids (:processed-ids watermark #{})
          comparable-ts (if (string? ts)
                          (string->instant ts)
                          ts)]
      (if (seq processed-ids)
               ;; Composite condition: ts > watermark OR (ts = watermark AND id NOT IN processed)
        {:or [{timestamp-column [:> comparable-ts]}
              {:and [{timestamp-column [:= comparable-ts]}
                     {id-column [:not-in processed-ids]}]}]}
               ;; Simple condition: ts > watermark
        {timestamp-column [:> comparable-ts]}))

        ;; Row-based watermark (CSV) - return nil, offset handled in fetch
    (and watermark (:last-row-number watermark))
    nil

        ;; No watermark
    :else
    nil))

(defn filter-already-processed
  "Filter out records that have already been processed.
       Used as a safety check in case the database doesn't support the composite query."
  [records watermark timestamp-column id-column]
  (if-not (and watermark (:last-timestamp watermark))
    records
    (let [last-ts (:last-timestamp watermark)
          processed-ids (:processed-ids watermark #{})]
      (filter (fn [record]
                (let [rec-ts (get record timestamp-column)
                      rec-id (get record id-column)]
                  (not (and (= rec-ts last-ts)
                            (contains? processed-ids rec-id)))))
              records))))

(defn watermark-stats
  "Get statistics about the watermark"
  [watermark]
  (when watermark
    (let [last-run-str (:last-run watermark)
          age-hours (when last-run-str
                      (try
                        (let [last-run (if (string? last-run-str)
                                         (java.time.Instant/parse last-run-str)
                                         last-run-str)
                              now (java.time.Instant/now)
                              duration (java.time.Duration/between last-run now)]
                          (.toHours duration))
                        (catch Exception e
                          nil)))]
      (merge
       {:last-run last-run-str
        :age-hours age-hours
        :metadata (:metadata watermark)}
                   ;; Add timestamp-based fields if present
       (when (:last-timestamp watermark)
         {:last-timestamp (:last-timestamp watermark)
          :processed-ids-count (count (:processed-ids watermark #{}))})
                   ;; Add row-based fields if present
       (when (:last-row-number watermark)
         {:last-row-number (:last-row-number watermark)})))))