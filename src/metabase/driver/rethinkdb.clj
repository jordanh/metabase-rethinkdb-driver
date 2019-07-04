(ns metabase.driver.rethinkdb
  (:require [clojure.tools.logging :as log]
            [metabase
              [util :as u]]
            [metabase.driver :as driver]
            [metabase.query-processor.store :as qp.store]
            [metabase.driver.rethinkdb
              [util :refer [details-to-rethinkdb-connection-params]]]))

(driver/register! :rethinkdb)

(defmethod driver/supports? [:rethinkdb :basic-aggregations] [_ _] false)

(defmethod driver/can-connect? :rethinkdb [_ details]
  (log/info (format "driver/can-connect? details: %s" details))
  (let [rethinkdb-params (details-to-rethinkdb-connection-params details)]
    (log/info (format "driver/can-connect? for rethinkdb: %s" rethinkdb-params))
    true))
