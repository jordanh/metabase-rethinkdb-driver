(ns metabase.driver.rethinkdb
  (:require [clojure.tools.logging :as log]
            [metabase
              [util :as u]]
            [metabase.driver :as driver]
            [metabase.query-processor.store :as qp.store]
            [rethinkdb.query :as r]
            [metabase.driver.rethinkdb
              [util :refer [*-to-clj-rethinkdb-params]]]))

(driver/register! :rethinkdb)

(defmethod driver/supports? [:rethinkdb :basic-aggregations] [_ _] false)

(defmethod driver/can-connect? :rethinkdb
  [_ details]
  (log/info (format "driver/can-connect? details: %s" details))
  (let [clj-rethinkdb-params (*-to-clj-rethinkdb-params details)]
    (log/info (format "driver/describe-database: clj-rethinkdb-params=%s" clj-rethinkdb-params))
    (with-open [conn (apply r/connect (mapcat identity clj-rethinkdb-params))]
      (-> (r/now) (r/gt 0) (r/run conn)))))

(defmethod driver/describe-database :rethinkdb
  [driver database]
  (log/info (format "driver/describe-database: driver=%s database=%s" driver database))
  (let [clj-rethinkdb-params (*-to-clj-rethinkdb-params database)]
    (log/info (format "driver/describe-database: clj-rethinkdb-params=%s" clj-rethinkdb-params))
    (with-open [conn (apply r/connect (mapcat identity clj-rethinkdb-params))]
      {:tables
        (set
          (for [table-name (-> (r/table-list) (r/run conn))]
            {:name table-name
            :schema nil}))})))
