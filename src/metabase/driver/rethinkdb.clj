(ns metabase.driver.rethinkdb
  (:require [clojure.tools.logging :as log]
            [metabase
              [util :as u]]
            [metabase.driver :as driver]
            [metabase.query-processor.store :as qp.store]
            [metabase.driver.rethinkdb
             [util :refer [with-rethinkdb-connection]]]))

(driver/register! :rethinkdb)

(defmethod driver/supports? [:rethinkdb :basic-aggregations] [_ _] false)

(defmethod driver/can-connect? :rethinkdb [_ details]
  (with-rethinkdb-connection _ details))