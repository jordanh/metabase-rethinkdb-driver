(ns metabase.driver.rethinkdb
  (:require [metabase.driver :as driver]
            [metabase.query-processor.store :as qp.store]))

(driver/register! :rethinkdb)

(defmethod driver/supports? [:rethinkdb :basic-aggregations] [_ _] false)

(defmethod driver/can-connect? :rethinkdb [_ _]
  true)
