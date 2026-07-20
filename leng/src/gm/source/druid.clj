(ns gm.source.druid
  (:require
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [cheshire.core :as json]
    [gm.source.core :as core]
    [clj-http.client :as http]))


(defrecord DruidAdapter
           [base-url           ; "http://druid-router:8888"
            sql-endpoint       ; usually "/druid/v2/sql"
            http-opts])