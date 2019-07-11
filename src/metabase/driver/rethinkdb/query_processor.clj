(ns metabase.driver.rethinkdb.query-processor
(:require   [clojure
             [string :as str]]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [metabase.driver.rethinkdb.util :refer [*rethinkdb-connection*]]
            [metabase.mbql
              [util :as mbql.u]]
            [metabase.models.field :refer [Field]]
            [metabase.query-processor
              [interface :as i]
              [store :as qp.store]]
            [metabase.query-processor.middleware.annotate :as annotate]
            [rethinkdb.query :as r])
    (:import metabase.models.field.FieldInstance))

;; See: https://github.com/metabase/metabase/wiki/(Incomplete)-MBQL-Reference

;;; ---------------------------------------- field mapping -----------------------------------------------------------

(defn- field->name
  "Return a single string name for FIELD. For nested fields, this creates a combined qualified name."
  ^String [^FieldInstance field, ^String separator]
  (if-let [parent-id (:parent_id field)]
    (str/join separator [(field->name (qp.store/field parent-id) separator)
                         (:name field)])
    (:name field)))

(defn fields->names [fields]
  (mapv #(-> (mbql.u/field-clause->id-or-literal %) 
             (qp.store/field)
             (field->name "."))
       fields))

(defmulti ^:private ->rvalue
  "Format this `Field` or value for use as the right hand value of an expression, e.g. by adding `$` to a `Field`'s
  name"
  {:arglists '([x])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmulti ^:private ->lvalue
  "Return an escaped name that can be used as the name of a given Field."
  {:arglists '([field])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmulti ^:private ->initial-rvalue
  "Return the rvalue that should be used in the *initial* projection for this `Field`."
  {:arglists '([field])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->lvalue         (class Field) [this] (field->name this "."))
; (defmethod ->initial-rvalue (class Field) [this] (str (field->name this ".")))
; (defmethod ->rvalue         (class Field) [this] (str (->lvalue this)))

(defmethod ->lvalue         :field-id [[_ field-id]] (->lvalue          (qp.store/field field-id)))
; (defmethod ->initial-rvalue :field-id [[_ field-id]] (->initial-rvalue  (qp.store/field field-id)))
; (defmethod ->rvalue         :field-id [[_ field-id]] (->rvalue          (qp.store/field field-id)))

(defmethod ->rvalue nil [_] nil)

(defmethod ->rvalue :value [[_ value _]] value)

;;; ---------------------------------------- table & field selection -------------------------------------------------


(defn- handle-table [{source-table-id :source-table}]
  (log/debug (format "driver.query-processor/handle-table: source-table-id=%s" source-table-id))
  (r/table (:name (qp.store/table source-table-id))))


(defn- handle-fields [query {:keys [fields]}]
  (log/debug (format "driver.query-processor/handle-fields: fields=%s" fields))
  ;; TODO: this will probably need to be enhanced to fully support nested fields
  (let [field-names (fields->names fields)]
    (-> query
        (r/pluck field-names))))

;;; ----------------------------------------------------- filter -----------------------------------------------------

(defmethod ->rvalue :not [[_ value]] (r/not (->rvalue value)))

(defmulti ^:private parse-filter (fn [filter-clause _] (first filter-clause)))

; (defn- str-match-pattern [options prefix value suffix]
;   (if (mbql.u/is-clause? ::not value)
;     (r/not (str-match-pattern options prefix (second value) suffix))
;     (let [case-sensitive? (get options :case-sensitive true)]
;       (re-pattern (str (when-not case-sensitive? "(?i)") prefix (->rvalue value) suffix)))))
; 
; (defmethod parse-filter :contains    [[_ field v opts]] {(->lvalue field) (str-match-pattern opts nil v nil)})
; (defmethod parse-filter :starts-with [[_ field v opts]] {(->lvalue field) (str-match-pattern opts \^  v nil)})
; (defmethod parse-filter :ends-with   [[_ field v opts]] {(->lvalue field) (str-match-pattern opts nil v \$)})

(defn- eq-filter [f field value row] (-> (r/get-field row (->lvalue field)) (f (->rvalue value))))

(defmethod parse-filter :between [[_ field min-val max-val] row]
  (r/and (eq-filter r/ge field min-val row) (eq-filter r/le field max-val row)))

(defmethod parse-filter :=  [[_ field value] row] (eq-filter r/eq field value row))
(defmethod parse-filter :!= [[_ field value] row] (eq-filter r/ne field value row))
(defmethod parse-filter :<  [[_ field value] row] (eq-filter r/lt field value row))
(defmethod parse-filter :>  [[_ field value] row] (eq-filter r/gt field value row))
(defmethod parse-filter :<= [[_ field value] row] (eq-filter r/le field value row))
(defmethod parse-filter :>= [[_ field value] row] (eq-filter r/ge field value row))

(defmethod parse-filter :and [[_ & args] row] (apply r/and (mapv #(parse-filter % row) args)))
(defmethod parse-filter :or  [[_ & args] row] (apply r/or (mapv #(parse-filter % row) args)))

(defn- handle-filter [query {filter-clause :filter}]
  (log/debug (format "driver.query-processor/handle-filter: filter-clause=%s" filter-clause))
  (if-not filter-clause
    query
    (r/filter query (r/fn [row] (parse-filter filter-clause row)))))

;;; ----------------------------------------------------- limit -----------------------------------------------------

(defn- handle-limit [query {:keys [limit]}]
  (log/debug (format "driver.query-processor/handle-limit: limit=%d" limit))
  (if-not limit
    query
    (-> query
        (r/limit limit))))

(defn add-results-xformation [query {:keys [fields]}]
  (log/debug (format "driver.query-processor/handle-results-xformation"))
  (let [field-names (fields->names fields)]
    (-> query
      (r/map (r/fn [row]
              (r/map field-names (r/fn [col] (r/default (r/get-field row col) nil))))))))

(defn- generate-rethinkdb-query
  [inner-query]
  (log/debug (format "driver.query-processor/generate-rethinkdb-query: inner-query=%s" inner-query))
  (let [inner-query (update inner-query :aggregation 
                      (partial mbql.u/pre-alias-and-uniquify-aggregations
                               annotate/aggregation-name))]
    (-> (handle-table inner-query)
        (handle-fields inner-query)
        (handle-filter inner-query)
        (handle-limit inner-query)
        (add-results-xformation inner-query))))

(defn mbql->native
  "Process and run an MBQL query."
  [query]
    ; (log/debug (format "driver.query-processor/mbql->native: query=%s" query))
  (let [native-query (generate-rethinkdb-query (:query query))]
    (log/debug (format "driver.query-processor/mbql->native: native-query=%s" native-query))
    native-query))

(defn execute-query
  "Process and run a native RethinkDB query."
  [{:keys [native query]}]
  (log/debug (format "driver.query-processor/execute-query: native=%s" (with-out-str (pprint native))))
  (let [field-names (fields->names (:fields query))]
    (try
      (let [rows (r/run native *rethinkdb-connection*)]
        (log/debug (format "driver.query-processor/execute-query: rows=%s" rows))
        {:columns field-names
         :rows rows})
    (catch Throwable t
      (log/error "Error running RethinkDB query" t)))))
  