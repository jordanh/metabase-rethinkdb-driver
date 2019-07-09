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

(defn- handle-fields [query {:keys [fields]}]
  (log/info (format "driver.query-processor/handle-fields: fields=%s" fields))
  (let [field-names (map #(-> (mbql.u/field-clause->id-or-literal %)
                              (qp.store/field)
                              (:name))
                         fields)]
    ;; TODO: this will probably need to be enhanced to fully support nested fields
    (-> query
        (r/with-fields field-names)
        (r/map (r/fn [row]
                 (r/map field-names (r/fn [col] (r/get-field row col))))))))

(defn- handle-limit [query {:keys [limit]}]
  (log/info (format "driver.query-processor/handle-limit: limit=%d" limit))
  (if-not limit
    query
    (-> query
        (r/limit limit))))

(defn- generate-rethinkdb-query
  [inner-query]
  (log/info (format "driver.query-processor/generate-rethinkdb-query: inner-query=%s" inner-query))
  (let [inner-query (update inner-query :aggregation 
                      (partial mbql.u/pre-alias-and-uniquify-aggregations
                               annotate/aggregation-name))]
    (-> {}
        (handle-table inner-query)
        (handle-fields inner-query)
        (handle-limit inner-query))}))

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
  