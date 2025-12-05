(ns gm.sink.druid
  (:require [gm.sink.core :as sink]
            [clj-http.client :as http]
            [cheshire.core :as json]))

(defrecord DruidAdapter [base-url datasource auth-config])

(defn- build-http-options
  "Build HTTP options with optional authentication.

        Supports:
        - Basic Auth: {:type :basic :username \"user\" :password \"pass\"}
        - Bearer Token: {:type :bearer :token \"jwt-token\"}"
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

(defn- records->druid-format
  "Convert records to Druid ingestion format"
  [records timestamp-column datasource]
  {:type "index_parallel"
   :spec {:dataSchema {:dataSource datasource
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
    (let [task-spec (records->druid-format records :timestamp table)
          url (str (:base-url this) "/druid/indexer/v1/task")
          http-opts (merge (build-http-options this :json)
                           {:body (json/generate-string task-spec)})
          response (http/post url http-opts)]
      (json/parse-string (:body response) true)))

  (update-records [this table conditions updates]    ; RENAMED
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
    (sink/insert this table records))

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