(ns gm.sink.druid
  (:require [gm.sink.core :as sink]
            [clj-http.client :as http]
            [cheshire.core :as json]
            [cheshire.generate :as json-gen]
            [clojure.tools.logging :as log])
  (:import [java.time Instant]
           [java.time.format DateTimeFormatter]))

;; Add custom JSON encoder for java.time.Instant
(json-gen/add-encoder java.time.Instant
                      (fn [instant jsonGenerator]
                        (.writeString jsonGenerator (.toString instant))))

(defrecord DruidAdapter [base-url datasource auth-config])

(defn- build-http-options
  "Build HTTP options with optional authentication"
  [adapter content-type]
  (let [base-opts {:content-type content-type}
        auth (:auth-config adapter)]
    (if auth
      (case (:type auth)
        :basic (assoc base-opts
                      :basic-auth [(:username auth) (:password auth)])
        :bearer (assoc base-opts
                       :headers {"Authorization" (str "Bearer " (:token auth))})
        base-opts)
      base-opts)))

(defn- convert-timestamps
  "Convert all timestamp fields to ISO-8601 strings"
  [record]
  (into {}
        (map (fn [[k v]]
               [k (cond
                    (instance? java.time.Instant v)
                    (.toString v)

                    (instance? java.util.Date v)
                    (.format (java.text.SimpleDateFormat. "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'") v)

                    (instance? java.sql.Timestamp v)
                    (.toString (.toInstant v))

                    :else v)])
             record)))

(defn- records->druid-format
  "Convert records to Druid ingestion format using index_parallel"
  [records timestamp-column datasource]
  (let [converted-records (map convert-timestamps records)
        dimensions (remove #{(keyword timestamp-column)}
                           (keys (first converted-records)))]
    {:type "index_parallel"
     :spec {:ioConfig {:type "index_parallel"
                       :inputSource {:type "inline"
                                     :data (clojure.string/join "\n"
                                                                (map json/generate-string converted-records))}
                       :inputFormat {:type "json"}}
            :tuningConfig {:type "index_parallel"
                           :partitionsSpec {:type "dynamic"}}
            :dataSchema {:dataSource datasource
                         :timestampSpec {:column (name timestamp-column)
                                         :format "auto"}
                         :dimensionsSpec {:dimensions (mapv name dimensions)}
                         :granularitySpec {:type "uniform"
                                           :segmentGranularity "DAY"
                                           :queryGranularity "NONE"
                                           :rollup false}}}}))

(extend-type DruidAdapter
  sink/SinkAdapter

  (insert [this table records]
    (let [task-spec (records->druid-format records :event_time table)
          url (str (:base-url this) "/druid/indexer/v1/task")
          http-opts (merge (build-http-options this :json)
                           {:body (json/generate-string task-spec)
                            :throw-exceptions false})]
      (log/info "Submitting Druid ingestion task for" (count records) "records")
      (try
        (let [response (http/post url http-opts)]
          (if (= 200 (:status response))
            (do
              (log/info "Druid task submitted successfully")
              (json/parse-string (:body response) true))
            (do
              (log/error "Druid ingestion failed with status:" (:status response))
              (log/error "Response body:" (:body response))
              (log/error "Task spec:" (json/generate-string task-spec {:pretty true}))
              (throw (ex-info "Druid ingestion failed"
                              {:status (:status response)
                               :body (:body response)
                               :task-spec task-spec})))))
        (catch Exception e
          (log/error "Exception during Druid ingestion:" (.getMessage e))
          (throw e)))))

  (update-records [this table conditions updates]
    (throw (ex-info "Druid doesn't support in-place updates"
                    {:operation :update})))

  (upsert [this table key-columns records]
    (sink/insert this table records))

  (delete [this table conditions]
    (let [interval (get conditions :interval)
          url (str (:base-url this) "/druid/coordinator/v1/datasources/"
                   (name table) "/intervals/" interval)
          http-opts (build-http-options this :json)]
      (http/delete url http-opts)))

  (create-table [this table schema]
    {:message "Druid datasource will be created on first ingestion"
     :datasource table
     :schema schema})

  (batch-insert [this table records batch-size]
                           ;; Druid does batch ingestion by default, so just call insert
    (doseq [batch (partition-all batch-size records)]
      (sink/insert this table batch)))

  (transaction [this operations]
    (throw (ex-info "Druid doesn't support transactions"
                    {:operation :transaction})))

  (connect [this config]
    (assoc this
           :base-url (:base-url config)
           :datasource (:datasource config)
           :auth-config (:auth config)))

  (disconnect [this]
    this))

(defn create-adapter
  "Create a Druid sink adapter"
  [config]
  (map->DruidAdapter (select-keys config [:base-url :datasource :auth])))