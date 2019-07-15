(ns metabase.driver.rethinkdb.query-processor
(:require   [clojure
             [set :as set]
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
            [metabase.util
              [date :as du]]
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

(defmulti ^:private ->lvalue
  "Return an escaped name that can be used as the name of a given Field."
  {:arglists '([field])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->lvalue         (class Field) [this] (field->name this "."))
(defmethod ->lvalue         :field-id [[_ field-id]] (->lvalue          (qp.store/field field-id)))

(defmulti ^:private ->rvalue
  "Format this `Field` or value for use as the right hand value of an expression, e.g. by adding `$` to a `Field`'s
  name"
  {:arglists '([x])}
  mbql.u/dispatch-by-clause-name-or-class)

(defmethod ->rvalue nil [_] nil)
(defmethod ->rvalue :value [[_ value _]] value)

(defmethod ->rvalue :not [[_ value]] (r/not (->rvalue value)))

(defmethod ->rvalue :datetime-field
  [[_ field]]
  (->rvalue field))

(defmethod ->rvalue :absolute-datetime
  [[_ timestamp unit]]
  (r/iso8601 (du/date->iso-8601 (if-not (= :default unit)
                             (du/date-trunc unit timestamp)
                             timestamp))))

; Following Druid driver's lead and treating times exactly like a date
(defmethod ->rvalue :time
  [[_ time unit]]
  (r/iso8601 (du/date->iso-8601 (if-not (= :default unit)
                             (du/date-trunc unit time)
                             time))))

(defmethod ->rvalue :relative-datetime
  [[_ amount unit]]
  (du/date->iso-8601 (du/date-trunc unit (du/relative-date unit amount))))

;;; ---------------------------------------- table & field selection -------------------------------------------------

(defn- handle-table [{source-table-id :source-table}]
  (let [table-name (:name (qp.store/table source-table-id))]
    (log/debug (format "driver.query-processor/handle-table: table-name=%s" table-name))
    (r/table table-name)))

(defn- aggregation-column-name
  [[_ [_] column-name]]
  column-name)

(defn- resolve-mbql-field-names
  "Given an MBQL query, return a vector of the field names to use whether the
   query is a simple query, a breakout or an aggregation (or a combination)."
  [query]
  (let [{:keys [aggregation fields breakout]} query]
    (cond
      aggregation (if breakout
                      (set/union
                        (fields->names breakout)
                        (mapv aggregation-column-name aggregation))
                      (mapv aggregation-column-name aggregation))
      breakout  (fields->names breakout)
      fields    (fields->names fields))))

(defn- handle-fields [query {:keys [fields]}]
  (log/debug (format "driver.query-processor/handle-fields: fields=%s" fields))
  (if-not fields
    query
    ;; TODO: this will probably need to be enhanced to fully support nested fields
    (let [field-names (fields->names fields)]
      (r/pluck query field-names))))

;;; ----------------------------------------------------- breakout ---------------------------------------------------

;; TODO: operate on multiple groups, N.B. our transformation at the end is going to be intense
;;       I think we should stop doing this on the database server...
(defn- handle-breakout [query {concrete-field-vec :breakout}]
  (log/debug (format "driver.query-processor/handle-breakout: concrete-field-vec=%s" concrete-field-vec))
  (if-not concrete-field-vec
    query
    (let [field-names (fields->names concrete-field-vec)]
      (-> ;; The MBQL spec implies there can only be one breakout field, but other drivers make it look like a vector
          ;; of fields can be specified. ReQL allows distinct() to be run on a sequence of objects just as easily
          ;; so we'll start there.
          (r/pluck query field-names)
          ;; TODO: if an index is available, we should probably use it. Not safe to just assume an index name tho...
          (r/distinct)))))

;;; ----------------------------------------------------- filter -----------------------------------------------------

(defmulti ^:private parse-filter (fn [filter-clause _] (first filter-clause)))

(defn- match-filter [field pattern row] (-> (r/get-field row (->lvalue field)) (r/match pattern)))

(defn- str-match-pattern [options prefix value suffix field row]
  (if (mbql.u/is-clause? ::not value)
    (r/not (str-match-pattern options prefix (second value) suffix field row))
    (let [case-sensitive? (get options :case-sensitive true)]
      (match-filter field
                    (re-pattern (str (when-not case-sensitive? "(?i)") prefix (->rvalue value) suffix))
                    row))))

(defmethod parse-filter :contains    [[_ field v opts] row] (str-match-pattern opts nil v nil field row))
(defmethod parse-filter :starts-with [[_ field v opts] row] (str-match-pattern opts \^  v nil field row))
(defmethod parse-filter :ends-with   [[_ field v opts] row] (str-match-pattern opts nil v \$ field row))

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

;;; ------------------------------------------------- aggregation ---------------------------------------------------

(defmulti ^:private parse-aggregation
  (fn [[_ [named] _] _ _]
  (if (coll? named) (first named) named)))

(defn- r-normalize-aggregation-result
  [query out-column-name]
  (-> (r/do query (r/fn [n] (r/object out-column-name n)))
      ; Take a single RethinkDB object and coerce it to a single element sequence.
      ; e.g. { count: 42 } -> [ {count: 42 } ]
      (r/coerce-to "array")
      (r/map (r/fn [i] (r/object (r/args i))))))

(defmethod parse-aggregation :avg [aggregation-clause out-column-name query] 
  (let [[_ [_ in-field] _] aggregation-clause
        in-column-name (->lvalue in-field)]
    (log/debug (format "driver.query-processor/parse-aggregation: in-column-name=%s" in-column-name))
    (-> (r/avg query in-column-name)
        (r-normalize-aggregation-result out-column-name))))
  
(defmethod parse-aggregation :count [_ out-column-name query] 
  (-> (r/count query)
      (r-normalize-aggregation-result out-column-name)))
  
  ; (r/map query (r/fn [_] (r/object column-name 1)))
  ;     (r/reduce (r/fn [left right]
  ;                 (r/object
  ;                   column-name
  ;                   (r/add
  ;                     (r/get-field left column-name)
  ;                     (r/get-field right column-name)))))
  ;     (r-aggregation-array-coersion)))

(defn- handle-aggregation
  [query {aggregation-clauses :aggregation}]
  ;; TODO handle one aggregation clause now, multiple in the future
  (log/debug (format "driver.query-processor/handle-aggregation: aggregation-clauses=%s" aggregation-clauses))
  (if-let [aggregation-clause (first aggregation-clauses)]
    (let [column-name (aggregation-column-name aggregation-clause)]
      (log/debug
        (format
          "driver.query-processor/handle-aggregation: aggregation-clause=%s column-name=%s"
          aggregation-clause column-name))
      (parse-aggregation aggregation-clause column-name query))
    query))

;;; ----------------------------------------------------- limit -----------------------------------------------------

(defn- handle-limit [query {:keys [limit]}]
  (log/debug (format "driver.query-processor/handle-limit: limit=%d" limit))
  (if-not limit
    query
    (r/limit query limit)))

;;; -------------------------------------------------- order by -----------------------------------------------------

;; TODO: handle multiple order-by clause
(defn handle-order-by [query {:keys [order-by]}]
  (log/debug (format "driver.query-processor/handle-order-by: order-by=%s" order-by))
  (if-not order-by
    query
    (let [[[direction field]] order-by]
    (log/debug (format "driver.query-processor/handle-order-by: direction=%s field=%s" direction field))
      ;; TODO: this would also be a great place to use an index
      (r/order-by query
                  (condp = direction
                    :asc  (r/asc (->lvalue field))
                    :desc (r/desc (->lvalue field)))))))

;;; ------------------------------------------- results xformation --------------------------------------------------

;; TODO support breakouts for real cool like
; r.db('actionDevelopment').table('Task')
;           vvvvvv    vvvvvvvvv -- breakouts
;   .group('status', 'sortOrder')
;                                                vvvvvvvvvvv -- fields
;   .map( (row) => r.map(['status', 'sortOrder', 'createdAt'], (col) => row(col)) )
;   .ungroup()
;   .concatMap( (row) => row('reduction') )

(defn add-results-xformation [query {:keys [fields breakout] :as mbql-query}]
  (let [field-names (resolve-mbql-field-names mbql-query)]
    (log/debug (format "driver.query-processor/add-results-xformation: field-names=%s" field-names))
    (r/map query (r/fn [row]
            (r/map field-names (r/fn [col] (r/default (r/get-field row col) nil)))))))

;;; ------------------------------------- core query parsing functions ----------------------------------------------

(defn- generate-rethinkdb-query
  [mbql-query]
  (let [mbql-query (update mbql-query :aggregation 
                      (partial mbql.u/pre-alias-and-uniquify-aggregations
                               annotate/aggregation-name))]
    (log/debug (format "driver.query-processor/generate-rethinkdb-query: mbql-query=%s" mbql-query))
    (-> (handle-table mbql-query)
        (handle-fields mbql-query)
        (handle-breakout mbql-query)
        (handle-filter mbql-query)
        (handle-aggregation mbql-query)
        (handle-limit mbql-query)
        (handle-order-by mbql-query)
        (add-results-xformation mbql-query)
        )))

(defn mbql->native
  "Process and run an MBQL query."
  [query]
  (let [native-query (generate-rethinkdb-query (:query query))]
    (log/debug (format "driver.query-processor/mbql->native: native-query=%s" native-query))
    native-query))

(defn execute-query
  "Process and run a native RethinkDB query."
  [{native :native mbql-query :query}]
  (log/debug (format "driver.query-processor/execute-query: native=%s" (with-out-str (pprint native))))
  (let [mbql-query (update mbql-query :aggregation 
                      (partial mbql.u/pre-alias-and-uniquify-aggregations
                               annotate/aggregation-name))
        field-names (resolve-mbql-field-names mbql-query)]
    (try
      (let [rows (r/run native *rethinkdb-connection*)]
        (log/debug (format "driver.query-processor/execute-query: rows=%s" rows))
        {:columns field-names
         :rows rows})
    (catch Throwable t
      (log/error "Error running RethinkDB query" t)))))
  