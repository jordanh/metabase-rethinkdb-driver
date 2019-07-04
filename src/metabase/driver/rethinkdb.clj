(ns metabase.driver.rethinkdb
  (:require [clojure.tools.logging :as log]
            [metabase
              [util :as u]]
            [metabase.driver :as driver]
            [metabase.query-processor.store :as qp.store])
  (:import (com.rethinkdb RethinkDB)))

(driver/register! :rethinkdb)

(def r RethinkDB/r)

(defn- details-to-rethinkdb-connection-params
  [{:keys [host dbname port]}]
    {:host        host
     :db          dbname
     :port        port
    })


(defn connect
  [details]
  (let  [connect-params (details-to-rethinkdb-connection-params details)
         {:keys [dbname host port]} connect-params]
    (-> (.connection r)
        (cond->
          (not-empty dbname) (.dbname dbname)
          (not-empty host) (.hostname host)
          (not-empty port) (.port port))
        .connect)))


(defmethod driver/supports? [:rethinkdb :basic-aggregations] [_ _] false)

(defmethod driver/can-connect? :rethinkdb [_ details]
  (log/info (format "driver/can-connect? details: %s" details))
  (-> (.expr r true) (.run (connect details))))
