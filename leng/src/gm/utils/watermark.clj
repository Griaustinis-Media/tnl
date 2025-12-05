(ns gm.utils.watermark
  "Watermark management for incremental data pipelines"
  (:require [clojure.java.io :as io]
            [clojure.edn :as edn]
            [clojure.tools.logging :as log]))

(defrecord Watermark [last-timestamp last-run metadata])

(defn load-watermark
  "Load the last processed timestamp from file.
       Returns a Watermark record or nil if no watermark exists."
  [watermark-file]
  (let [file (io/file watermark-file)]
    (if (.exists file)
      (try
        (let [data (edn/read-string (slurp file))]
          (map->Watermark data))
        (catch Exception e
          (log/error "Failed to load watermark file:" (.getMessage e))
          nil))
      nil)))

(defn save-watermark
  "Save the watermark to file.
       timestamp: The watermark timestamp value
       watermark-file: Path to the watermark file
       metadata: Optional metadata map to store with watermark"
  ([watermark-file timestamp]
   (save-watermark watermark-file timestamp {}))
  ([watermark-file timestamp metadata]
   (let [watermark {:last-timestamp timestamp
                    :last-run (str (java.time.Instant/now))
                    :metadata metadata}]
     (try
       (spit watermark-file (pr-str watermark))
       (log/info "Saved watermark:" watermark)
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

(defn find-max-timestamp
  "Find the maximum timestamp from a collection of records.
       Handles java.time.Instant, java.util.Date, and numeric timestamps.

       records: Collection of records (maps)
       timestamp-column: Keyword for the timestamp column"
  [records timestamp-column]
  (when (seq records)
    (let [timestamps (map #(get % timestamp-column) records)]
      (reduce (fn [max-ts ts]
                (cond
                  (nil? max-ts) ts
                  (nil? ts) max-ts

                               ;; Handle java.time.Instant
                  (and (instance? java.time.Instant max-ts)
                       (instance? java.time.Instant ts))
                  (if (.isAfter ts max-ts) ts max-ts)

                               ;; Handle java.util.Date
                  (and (instance? java.util.Date max-ts)
                       (instance? java.util.Date ts))
                  (if (.after ts max-ts) ts max-ts)

                               ;; Handle numeric timestamps (millis)
                  (and (number? max-ts) (number? ts))
                  (max max-ts ts)

                               ;; Handle mixed types - convert to millis for comparison
                  :else
                  (let [max-millis (cond
                                     (instance? java.time.Instant max-ts)
                                     (.toEpochMilli max-ts)

                                     (instance? java.util.Date max-ts)
                                     (.getTime max-ts)

                                     (number? max-ts)
                                     max-ts

                                     :else
                                     (throw (ex-info "Unsupported timestamp type"
                                                     {:type (type max-ts)})))
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
                    (if (> ts-millis max-millis) ts max-ts))))
              nil
              timestamps))))

(defn find-min-timestamp
  "Find the minimum timestamp from a collection of records.
       Similar to find-max-timestamp but returns the earliest timestamp."
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
       Returns a condition that can be passed to query builders.

       watermark: Watermark record or nil
       timestamp-column: Keyword for the timestamp column

       Example:
       (build-incremental-condition watermark :event_ts)
       => {:event_ts [:> #inst \"2024-12-05T10:00:00.000Z\"]}"
  [watermark timestamp-column]
  (when (and watermark (:last-timestamp watermark))
    {timestamp-column [:> (:last-timestamp watermark)]}))

(defn watermark-stats
  "Get statistics about the watermark"
  [watermark]
  (when watermark
    {:last-timestamp (:last-timestamp watermark)
     :last-run (:last-run watermark)
     :age-hours (when (:last-run watermark)
                  (try
                    (let [last-run (java.time.Instant/parse (:last-run watermark))
                          now (java.time.Instant/now)
                          duration (java.time.Duration/between last-run now)]
                      (.toHours duration))
                    (catch Exception e
                      nil)))
     :metadata (:metadata watermark)}))