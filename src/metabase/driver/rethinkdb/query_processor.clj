(ns metabase.driver.rethinkdb.query-processor
  (:require [clojure.tools.logging :as log]))

;; See: https://github.com/metabase/metabase/wiki/(Incomplete)-MBQL-Reference

(defn mbql->native
  "Process and run an MBQL query."
  [{{source-table-id :source-table} :query, :as query}]
  (log/info (format "driver.query-processor/mbql->native: query=%s" query)))

(defn execute-query
  "Process and run a native RethinkDB query."
  [{{:keys [collection query mbql? projections]} :native}]
  (log/info (format "driver.query-processor/execute-query: query=%s" query)))
  