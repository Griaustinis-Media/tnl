(ns gm.source.cassandra
  (:require
   [gm.source.core :as core]
   [qbits.alia :as alia]
   [qbits.hayt :as hayt]))

(defrecord CassandraAdapter [session])

(defn- convert-conditions
  "Convert conditions to Cassandra WHERE format"
  [conditions]
  (into {}
        (map (fn [condition]
               (if (map? condition)
                 (first condition)
                 condition))
             conditions)))

(defn- build-session-config
  "Build session configuration with optional authentication"
  [config]
  (let [base-config {:contact-points (:contact-points config)
                     :port (or (:port config) 9042)}]
    (if (and (:username config) (:password config))
      (assoc base-config
             :credentials {:user (:username config)
                           :password (:password config)})
      base-config)))

(defn fetch
  "Fetch data from Cassandra based on query spec"
  [adapter {:keys [table columns conditions limit-n ordering]}]
  (let [query (cond-> (hayt/select (keyword table))
                          ;; Columns
                (and columns (not= columns [:*]))
                (hayt/columns (vec columns))

                          ;; WHERE conditions
                (seq conditions)
                (hayt/where (convert-conditions conditions))

                          ;; ORDER BY
                (seq ordering)
                (hayt/order-by (vec ordering))

                          ;; LIMIT
                limit-n
                (hayt/limit limit-n))]

    (alia/execute (:session adapter) query)))

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
    (when-let [ks (:keyspace config)]
      (alia/execute session (str "USE " ks)))
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
   :conditions []
   :ordering []})

(defn select-columns
  "Specify columns to select"
  [spec columns]
  (assoc spec :columns columns))

(defn where
  "Add filter condition"
  [spec condition]
  (update spec :conditions conj condition))

(defn limit
  "Limit results"
  [spec n]
  (assoc spec :limit-n n))

(defn order-by
  "Order results"
  [spec ordering]
  (assoc spec :ordering ordering))