(ns metabase.driver.rethinkdb.query-processor
  (:require [clojure.tools.logging :as log]
            [metabase.mbql
              [util :as mbql.u]]
            [metabase.query-processor
              [interface :as i]
              [store :as qp.store]]
            [metabase.query-processor.middleware.annotate :as annotate]
            [rethinkdb.query :as r]))

;; See: https://github.com/metabase/metabase/wiki/(Incomplete)-MBQL-Reference

(defn- handle-table [query {source-table-id :source-table}]
  (log/info (format "driver.query-processor/handle-table: source-table-id=%s" source-table-id))
  (if-not source-table-id
    query
    (r/table query (qp.store/table source-table-id))))

(defn- generate-rethinkdb-query
  [inner-query]
  (log/info (format "driver.query-processor/generate-rethinkdb-query: inner-query=%s" inner-query))
  (let [inner-query (update inner-query :aggregation 
                      (partial mbql.u/pre-alias-and-uniquify-aggregations
                               annotate/aggregation-name))]
    {:query (-> {}
                (handle-table inner-query))}))

(defn mbql->native
  "Process and run an MBQL query."
  [query]
    ; (log/info (format "driver.query-processor/mbql->native: query=%s" query))
  (let [native-query (generate-rethinkdb-query (:query query))]
    (log/info (format "driver.query-processor/mbql->native: native-query=%s" native-query))
    {:query native-query
     :mbql? true}))

(defn execute-query
  "Process and run a native RethinkDB query."
  [{{:keys [mbql? query]} :native}]
  (log/info (format "driver.query-processor/execute-query: query=%s" query)))
  