(ns gm.source.cassandra
  (:require
   [clojure.tools.logging :as log]
   [gm.source.core :as core]
   [qbits.alia :as alia]
   [qbits.hayt :as hayt]))

(defrecord CassandraAdapter [session])

(defn- build-contact-points
  "Build contact points with port if specified"
  [config]
  (let [hosts (:contact-points config)
        port (or (:port config) 9042)]
    (mapv #(str % ":" port) hosts)))

(defn- build-session-config
  "Build session configuration with optional authentication.

        For Alia 5.0.0:
        - :contact-points should be [\"host:port\"] format
        - :load-balancing-local-datacenter is required when using contact points
        - Authentication requires:
          * :auth-provider-class (use PlainTextAuthProvider)
          * :auth-provider-user-name
          * :auth-provider-password
        - :session-keyspace sets the keyspace"
  [config]
  (let [base-config {:contact-points (build-contact-points config)
                          ;; Required when using explicit contact points
                     :load-balancing-local-datacenter (or (:datacenter config) "datacenter1")}]
    (cond-> base-config
                    ;; Add authentication if provided
      (and (:username config) (:password config))
      (assoc :auth-provider-class "PlainTextAuthProvider"
             :auth-provider-user-name (:username config)
             :auth-provider-password (:password config))

                    ;; Add keyspace if provided
      (:keyspace config)
      (assoc :session-keyspace (:keyspace config)))))

(defn fetch
  "Fetch data from Cassandra based on query spec"
  [adapter {:keys [table columns conditions limit-n ordering allow-filtering?]}]
  (let [;; Build the raw CQL query string
        col-str (if (= columns [:*])
                  "*"
                  (clojure.string/join ", " (map name columns)))
        where-clauses (when (seq conditions)
                        (map (fn [[k v]]
                               (if (vector? v)
                                           ;; Handle operators like [:> value]
                                 (let [[op val] v]
                                   (str (name k) " " (name op) " ?"))
                                           ;; Simple equality
                                 (str (name k) " = ?")))
                             conditions))
        where-str (when (seq where-clauses)
                    (str " WHERE " (clojure.string/join " AND " where-clauses)))
        limit-str (when limit-n
                    (str " LIMIT " limit-n))
        allow-filtering-str (when allow-filtering?
                              " ALLOW FILTERING")
        cql (str "SELECT " col-str " FROM " (name table)
                 where-str limit-str allow-filtering-str)
        params (when (seq conditions)
                 (vec (map (fn [[k v]]
                             (if (vector? v)
                               (second v)  ; Extract value from [:> value]
                               v))
                           conditions)))]

    (log/info "Executing CQL:" cql)
    (log/info "With params:" params)

    (if params
      (alia/execute (:session adapter) cql {:values params})
      (alia/execute (:session adapter) cql))))

(defn fetch-lazy
  "Fetch data lazily for streaming"
  [adapter query-spec]
  (let [query-map (select-keys query-spec [:table :columns :conditions :limit-n :ordering])]
    (fetch adapter query-map)))

(defn connect
  "Connect to Cassandra"
  [adapter config]
  (let [session-config (build-session-config config)
        session (alia/session session-config)]
    (assoc adapter :session session)))

(defn disconnect
  "Disconnect from Cassandra"
  [adapter]
  (when-let [session (:session adapter)]
    (alia/close session))
  adapter)

(defn create-adapter
  "Create a Cassandra source adapter"
  []
  (map->CassandraAdapter {}))

;; Query spec helpers
(defn query-spec
  "Create a query specification"
  [table]
  {:table table
   :columns [:*]
   :conditions {}
   :ordering []})

(defn where
  "Add filter condition. Accepts a map of column->value pairs."
  [spec condition]
  (update spec :conditions merge condition))

(defn limit
  "Limit results"
  [spec n]
  (assoc spec :limit-n n))

(defn order-by
  "Order results"
  [spec ordering]
  (assoc spec :ordering ordering))

(defn select-columns
  "Specify columns to select"
  [spec columns]
  (assoc spec :columns columns))

(defn allow-filtering
  "Enable ALLOW FILTERING for the query"
  [spec]
  (assoc spec :allow-filtering? true))