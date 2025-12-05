(ns gm.sink.druid
  (:require [gm.sink.core :as sink]
            [clj-http.client :as http]
            [cheshire.core :as json]))

(defrecord DruidAdapter [base-url datasource])

(defn- records->druid-format
  "Convert records to Druid ingestion format"
  [records timestamp-column]
  {:type "index_parallel"
   :spec {:dataSchema {:dataSource (:datasource records)
                       :timestampSpec {:column (name timestamp-column)
                                       :format "auto"}
                       :dimensionsSpec {:dimensions (keys (first records))}
                       :metricsSpec []}
          :ioConfig {:type "inline"
                     :inputSource {:type "inline"
                                   :data (json/generate-string records)}}
          :tuningConfig {:type "index_parallel"}}})

(extend-type DruidAdapter
  sink/SinkAdapter

  (insert [this table records]
                     ;; Druid uses batch ingestion via supervisor/task
    (let [task-spec (records->druid-format
                     (assoc this :datasource table)
                     :timestamp)
          url (str (:base-url this) "/druid/indexer/v1/task")
          response (http/post url
                              {:body (json/generate-string task-spec)
                               :content-type :json})]
      (json/parse-string (:body response) true)))

  (update [this table conditions updates]
                     ;; Druid doesn't support traditional updates
                     ;; Would need to re-ingest data
    (throw (ex-info "Druid doesn't support in-place updates"
                    {:operation :update})))

  (upsert [this table key-columns records]
                     ;; Druid handles this via rollup during ingestion
    (sink/insert this table records))

  (delete [this table conditions]
                     ;; Druid supports deletion via kill tasks
    (let [interval (get conditions :interval) ; e.g. "2024-01-01/2024-01-02"
          url (str (:base-url this) "/druid/coordinator/v1/datasources/"
                   (name table) "/intervals/" interval)]
      (http/delete url)))

  (create-table [this table schema]
                           ;; Druid creates datasources on first ingestion
                           ;; Schema is defined in the ingestion spec
    {:message "Druid datasource will be created on first ingestion"
     :datasource table
     :schema schema})

  (batch-insert [this table records batch-size]
                           ;; Druid always does batch inserts
    (sink/insert this table records))

  (transaction [this operations]
                          ;; Druid doesn't support transactions
    (throw (ex-info "Druid doesn't support transactions"
                    {:operation :transaction})))

  (connect [this config]
    (assoc this :base-url (:base-url config)
           :datasource (:datasource config)))

  (disconnect [this]
    this))

(defn create-adapter
  "Create a Druid sink adapter"
  [config]
  (map->DruidAdapter config))