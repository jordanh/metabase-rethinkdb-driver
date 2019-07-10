(ns metabase.driver.rethinkdb.query-processor
  (:require [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [metabase.driver.rethinkdb.util :refer [*rethinkdb-connection*]]
            [metabase.mbql
              [util :as mbql.u]]
            [metabase.query-processor
              [interface :as i]
              [store :as qp.store]]
            [metabase.query-processor.middleware.annotate :as annotate]
            [rethinkdb.query :as r]))

;; See: https://github.com/metabase/metabase/wiki/(Incomplete)-MBQL-Reference

(defn- handle-table [{source-table-id :source-table}]
  (log/info (format "driver.query-processor/handle-table: source-table-id=%s" source-table-id))
  (r/table (:name (qp.store/table source-table-id))))

(defn field-ids->names [fields]
  (map #(-> (mbql.u/field-clause->id-or-literal %) 
            (qp.store/field)
            (:name))
       fields))

(defn- handle-fields [query {:keys [fields]}]
  (log/info (format "driver.query-processor/handle-fields: fields=%s" fields))
  ;; TODO: this will probably need to be enhanced to fully support nested fields
  (let [field-names (field-ids->names fields)]
    (-> query
        (r/pluck field-names))))

(defn- handle-limit [query {:keys [limit]}]
  (log/info (format "driver.query-processor/handle-limit: limit=%d" limit))
  (if-not limit
    query
    (-> query
        (r/limit limit))))

(defn handle-results-xformation [query {:keys [fields]}]
  (log/info (format "driver.query-processor/handle-results-xformation"))
  (let [field-names (field-ids->names fields)]
    (-> query
      (r/map (r/fn [row]
              (r/map field-names (r/fn [col] (r/default (r/get-field row col) nil))))))))

(defn- generate-rethinkdb-query
  [inner-query]
  (log/info (format "driver.query-processor/generate-rethinkdb-query: inner-query=%s" inner-query))
  (let [inner-query (update inner-query :aggregation 
                      (partial mbql.u/pre-alias-and-uniquify-aggregations
                               annotate/aggregation-name))]
    (-> (handle-table inner-query)
        (handle-fields inner-query)
        (handle-limit inner-query)
        (handle-results-xformation inner-query))))

(defn mbql->native
  "Process and run an MBQL query."
  [query]
    ; (log/info (format "driver.query-processor/mbql->native: query=%s" query))
  (let [native-query (generate-rethinkdb-query (:query query))]
    (log/info (format "driver.query-processor/mbql->native: native-query=%s" native-query))
    native-query))

(defn execute-query
  "Process and run a native RethinkDB query."
  [{:keys [native query]}]
  (log/info (format "driver.query-processor/execute-query: native=%s" (with-out-str (pprint native))))
  (let [field-names (field-ids->names (:fields query))]
    (try
      (let [rows (r/run native *rethinkdb-connection*)]
        (log/info (format "driver.query-processor/execute-query: rows=%s" rows))
        {:columns field-names
         :rows rows})
    (catch Throwable t
      (log/error "Error running RethinkDB query" t)))))
  