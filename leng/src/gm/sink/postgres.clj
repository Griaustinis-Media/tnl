(ns gm.sink.postgres
  (:require [gm.sink.core :as sink]
            [clojure.java.jdbc :as jdbc])
  (:import [org.postgresql.util PGobject]))

(defrecord PostgresAdapter [db-spec connection])

(defn- records->insert-maps
  "Convert records to JDBC insert format"
  [records]
  (if (map? (first records))
    records
    (throw (ex-info "Records must be maps" {:records records}))))

(defn- build-where-clause
  "Build WHERE clause from conditions"
  [conditions]
  (if (empty? conditions)
    [""]
    (let [clauses (map first conditions)
          params (mapcat rest conditions)]
      (into [(str "WHERE " (clojure.string/join " AND " clauses))]
            params))))

(defn- build-db-spec
  "Build PostgreSQL database specification with authentication.

        Config options:
        - :host - Database host (default: localhost)
        - :port - Database port (default: 5432)
        - :dbname - Database name (required)
        - :user - Username (required)
        - :password - Password (required)
        - :ssl - Enable SSL connection (default: false)
        - :sslfactory - SSL factory class (optional)

        Additional JDBC options can be passed and will be included in the spec.

        Example:
        {:host \"localhost\"
         :port 5432
         :dbname \"mydb\"
         :user \"postgres\"
         :password \"secret\"
         :ssl true}"
  [config]
  (merge
   {:dbtype "postgresql"
    :dbname (:dbname config)
    :host (or (:host config) "localhost")
    :port (or (:port config) 5432)
    :user (:user config)
    :password (:password config)}
         ;; Include any additional JDBC options
   (select-keys config [:ssl :sslfactory :sslmode :sslcert :sslkey :sslrootcert
                        :currentSchema :loginTimeout :connectTimeout
                        :socketTimeout :ApplicationName])))

(extend-type PostgresAdapter
  sink/SinkAdapter

  (insert [this table records]
    (jdbc/insert-multi! (:db-spec this) table (records->insert-maps records)))

  (update-records [this table conditions updates]
    (let [[where-clause & params] (build-where-clause conditions)
          set-clause (clojure.string/join ", "
                                          (map #(str (name (key %)) " = ?") updates))
          sql (str "UPDATE " (name table) " SET " set-clause " " where-clause)]
      (jdbc/execute! (:db-spec this)
                     (into [sql] (concat (vals updates) params)))))

  (upsert [this table key-columns records]
    (let [cols (keys (first records))
          key-set (set key-columns)
          update-cols (remove key-set cols)
          col-names (clojure.string/join ", " (map name cols))
          placeholders (clojure.string/join ", " (repeat (count cols) "?"))
          conflict-cols (clojure.string/join ", " (map name key-columns))
          update-clause (clojure.string/join ", "
                                             (map #(str (name %) " = EXCLUDED." (name %)) update-cols))
          sql (str "INSERT INTO " (name table)
                   " (" col-names ") VALUES (" placeholders ") "
                   "ON CONFLICT (" conflict-cols ") "
                   "DO UPDATE SET " update-clause)]
      (doseq [record records]
        (jdbc/execute! (:db-spec this)
                       (into [sql] (map #(get record %) cols))))))

  (delete [this table conditions]
    (let [[where-clause & params] (build-where-clause conditions)
          sql (str "DELETE FROM " (name table) " " where-clause)]
      (jdbc/execute! (:db-spec this) (into [sql] params))))

  (create-table [this table schema]
    (let [cols (clojure.string/join ", "
                                    (map (fn [[col-name col-type]]
                                           (str (name col-name) " " (name col-type)))
                                         schema))
          sql (str "CREATE TABLE IF NOT EXISTS " (name table) " (" cols ")")]
      (jdbc/execute! (:db-spec this) [sql])))

  (batch-insert [this table records batch-size]
    (doseq [batch (partition-all batch-size records)]
      (sink/insert this table batch)))

  (transaction [this operations]
    (jdbc/with-db-transaction [tx (:db-spec this)]
      (doseq [op operations]
        (op))))

  (connect [this config]
    (let [db-spec (build-db-spec config)]
                           ;; Test connection
      (jdbc/with-db-connection [conn db-spec]
        (jdbc/query conn ["SELECT 1"]))
      (assoc this :db-spec db-spec)))

  (disconnect [this]
    (when-let [conn (:connection this)]
      (.close conn))
    (dissoc this :connection)))

(defn create-adapter
  "Create a PostgreSQL sink adapter.

       Config options:
       - :host - Database host (default: \"localhost\")
       - :port - Database port (default: 5432)
       - :dbname - Database name (required)
       - :user - Username (required)
       - :password - Password (required)
       - :ssl - Enable SSL connection (default: false)
       - :sslmode - SSL mode: disable, allow, prefer, require, verify-ca, verify-full

       Examples:

       ;; Basic connection
       (create-adapter {:host \"localhost\"
                        :port 5432
                        :dbname \"mydb\"
                        :user \"postgres\"
                        :password \"secret\"})

       ;; With SSL
       (create-adapter {:host \"prod-db.example.com\"
                        :port 5432
                        :dbname \"production\"
                        :user \"app_user\"
                        :password \"secure_password\"
                        :ssl true
                        :sslmode \"require\"})

       ;; With additional options
       (create-adapter {:host \"localhost\"
                        :dbname \"mydb\"
                        :user \"postgres\"
                        :password \"secret\"
                        :currentSchema \"public\"
                        :ApplicationName \"my-pipeline\"
                        :connectTimeout 10})"
  [config]
  (when-not (:dbname config)
    (throw (ex-info "Database name (:dbname) is required" {:config config})))
  (when-not (:user config)
    (throw (ex-info "Username (:user) is required" {:config config})))
  (when-not (:password config)
    (throw (ex-info "Password (:password) is required" {:config config})))
  (map->PostgresAdapter {:db-spec (build-db-spec config)}))