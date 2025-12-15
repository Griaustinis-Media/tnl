(ns gm.source.druid
  (:require [clj-http.client :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [gm.source.core :as core]))

(defrecord DruidAdapter [base-url sql-endpoint http-opts])

(defn create-adapter
  "Creates a new Druid adapter."
  []
  (->DruidAdapter "http://localhost:8888" "/druid/v2/sql" {}))

(defn connect
  "Connect to a Druid cluster."
  [adapter config]
  (let [base-url (:base-url config)
        sql-endpoint (:sql-endpoint config)
        http-opts (:http-opts config)]
    (log/info "Connecting to Druid at" base-url)
    (assoc adapter
           :base-url base-url
           :sql-endpoint sql-endpoint
           :http-opts http-opts)))

(defn disconnect
  "Disconnect from Druid cluster."
  [adapter]
  (log/info "Disconnected from Druid")
  adapter)

(defn query-spec
  "Create a query specification for Druid"
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

(defn limit
  "Add LIMIT to query"
  [query-spec n]
  (assoc query-spec :limit n))

(defn offset
  "Add OFFSET to query"
  [query-spec n]
  (assoc query-spec :offset n))

(defn allow-filtering
  "No-op for Druid (always allows filtering)"
  [query-spec]
  query-spec)

(defn- build-sql
  "Build SQL query from query-spec"
  [{:keys [table columns where limit offset]}]
  (let [cols (if (empty? columns) "*" (clojure.string/join ", " (map name columns)))
        quote-val (fn [v] (if (number? v) v (str "'" (clojure.string/replace v "'" "''") "'")))]
    (str "SELECT " cols
         " FROM " table
         (when (not-empty where)
           (str " WHERE "
                (clojure.string/join " AND "
                                     (for [[k v] where]
                                       (if (vector? v)
                                         (str (name k) " IN " "(" (clojure.string/join ", " (map quote-val (second v))) ")")
                                         (str (name k) " = " (quote-val v)))))))
         (when limit (str " LIMIT " limit))
         (when offset (str " OFFSET " offset)))))

(defn fetch
  "Fetch records from Druid based on query specification"
  [adapter query-spec]
  (let [{:keys [base-url sql-endpoint http-opts]} adapter
        sql (build-sql query-spec)
        query (json/generate-string {:query sql})
        url (str base-url sql-endpoint)]
    (log/info "Executing Druid query:" sql)
    (try
      (let [response (http/post url (merge {:body query
                                            :content-type :json
                                            :as :json}
                                           http-opts))
            data (:body response)]
        (log/info "Fetched" (count data) "records from Druid")
        (vec data))
      (catch Exception e
        (log/error "Error fetching from Druid:" (.getMessage e))
        (throw (ex-info "Error fetching from Druid" {:query sql} e))))))

(defn fetch-lazy
  "Fetch data lazily for streaming (same as fetch for Druid)"
  [adapter query-spec]
  (fetch adapter query-spec))