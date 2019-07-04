(ns metabase.driver.rethinkdb
  (:require [clojure.tools.logging :as log]
            [metabase
              [util :as u]]
            [metabase.driver :as driver]
            [metabase.query-processor.store :as qp.store]
            [rethinkdb.query :as r]
            [metabase.driver.rethinkdb
              [util :refer [details-to-clj-rethinkdb-params]]]))

(driver/register! :rethinkdb)

(defmethod driver/supports? [:rethinkdb :basic-aggregations] [_ _] false)

(defmethod driver/can-connect? :rethinkdb [_ details]
  (log/info (format "driver/can-connect? details: %s" details))
  (let [clj-rethinkdb-params (details-to-clj-rethinkdb-params details)]
    (with-open [conn (apply r/connect (mapcat identity clj-rethinkdb-params))]
      (-> (r/now) (r/gt 0) (r/run conn)))))
