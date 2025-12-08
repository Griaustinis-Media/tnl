(ns gm.sink.dunwich
  "Dunwich batch ingestion sink adapter"
  (:require [clojure.data.json :as json]
            [clj-http.client :as http]
            [gm.sink.core :as sink]
            [clojure.tools.logging :as log]))

(defrecord DunwichAdapter [base-url api-key])

(defn create-adapter
  "Create a Dunwich sink adapter"
  [config]
  (->DunwichAdapter
   (:base-url config)
   (:api-key config)))

(defn- make-request
  "Make HTTP request to Dunwich API"
  [adapter endpoint payload]
  (let [url (str (:base-url adapter) endpoint)
        headers (cond-> {"Content-Type" "application/json"}
                  (:api-key adapter)
                  (assoc "Authorization" (str "Bearer " (:api-key adapter))))
        options {:headers headers
                 :body (json/write-str payload)
                 :content-type :json
                 :accept :json
                 :throw-exceptions false}]

    (log/debug "POST" url)
    (let [response (http/post url options)]
      (log/debug "Response status:" (:status response))
      response)))

(defn- prepare-batch-payload
  "Prepare records for batch ingestion"
  [table records]
  {:table (name table)
   :records records})

;; Implement the SinkAdapter protocol
(extend-type DunwichAdapter
  sink/SinkAdapter

  (connect [adapter config]
    (let [base-url (:base-url config)
          api-key (:api-key config)]
      (when-not base-url
        (throw (ex-info "Dunwich base URL is required" {:config config})))

      (log/info "Connected to Dunwich:" base-url)
      (assoc adapter
             :base-url base-url
             :api-key api-key)))

  (disconnect [adapter]
    (log/info "Disconnected from Dunwich")
    adapter)

  (reserved-columns [adapter]
    #{})

  (insert [adapter table records]
    (when (seq records)
      (let [payload (prepare-batch-payload table records)
            response (make-request adapter "/v1/batch/ingest" payload)]

        (if (= 200 (:status response))
          (do
            (log/info "Successfully ingested" (count records) "records to" table)
            {:success true
             :records-written (count records)
             :response (json/read-str (:body response) :key-fn keyword)})
          (do
            (log/error "Failed to ingest records:" (:status response) (:body response))
            (throw (ex-info "Dunwich ingestion failed"
                            {:status (:status response)
                             :body (:body response)
                             :records (count records)})))))))

  (batch-insert [adapter table records batch-size]
    (let [batches (partition-all batch-size records)
          results (atom [])]

      (log/info "Ingesting" (count records) "records in" (count batches) "batches")

      (doseq [[idx batch] (map-indexed vector batches)]
        (log/info "Batch" (inc idx) "/" (count batches) ":" (count batch) "records")
        (let [result (insert adapter table batch)]
          (swap! results conj result)))

      {:success true
       :records-written (count records)
       :batches (count batches)
       :results @results}))

  (update-records [adapter table conditions updates]
    (throw (ex-info "Update operations not supported for Dunwich sink"
                    {:adapter adapter})))

  (upsert [adapter table key-columns records]
    (throw (ex-info "Upsert operations not supported for Dunwich sink"
                    {:adapter adapter})))

  (delete [adapter table conditions]
    (throw (ex-info "Delete operations not supported for Dunwich sink"
                    {:adapter adapter})))

  (create-table [adapter table schema]
    (log/warn "create-table is a no-op for Dunwich")
    {:success true})

  (transaction [adapter operations]
    (log/warn "Transactions not supported for Dunwich, executing operations sequentially")
    (doseq [op operations]
      ((:fn op)))))
