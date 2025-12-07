(ns gm.pipeline.core
  "Core pipeline utilities for generated pipelines"
  (:import (java.text SimpleDateFormat)
           (java.time Instant)
           (java.util Date)))

(defn build-where-clause
  "Convert condition maps to source adapter WHERE format"
  [conditions]
  (reduce
   (fn [acc condition]
     (case (:type condition)
       (:in :in_expression)
       (merge acc {(:column condition)
                   (if (:negated condition)
                     [:not-in (:values condition)]
                     [:in (:values condition)])})
                  ;; Default: simple comparison
       (merge acc {(:column condition)
                   (if (= (:operator condition) "=")
                     (:value condition)
                     [(keyword (:operator condition)) (:value condition)])})))
   {}
   conditions))

(defn make-transform-record
  [config]
  (fn [record]
    (let [pipeline-cfg (:pipeline config)]
      (-> record
        (update (:timestamp-column pipeline-cfg)
                #(cond
                   (instance? java.time.Instant %)
                   (.toString %)

                   (instance? java.util.Date %)
                   (.format (java.text.SimpleDateFormat.
                             "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
                            %)

                   :else %))
        (assoc :source (:source-type pipeline-cfg)
               :ingestion_time (System/currentTimeMillis))))))

(defn transform-timestamp
  "Convert various timestamp types to ISO-8601 string"
  [ts]
  (cond
    (instance? Instant ts)
    (.toString ts)

    (instance? Date ts)
    (.format (SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") ts)

    :else ts))

(defn add-metadata
  "Add standard metadata fields to a record"
  [record source-type ingestion-time]
  (assoc record
         :source source-type
         :ingestion_time ingestion-time))