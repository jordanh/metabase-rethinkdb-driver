(ns metabase.driver.rethinkdb.query-processor
(:require   [clojure
             [set :as set]
             [string :as str]]
            [clojure.tools.logging :as log]
            [clojure.pprint :refer [pprint]]
            [metabase.driver.rethinkdb.util
              :refer [*rethinkdb-connection* apply_args_to_l_r_fn_pairs]]
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
      (> (count aggregation) 0) (if breakout
                                  (set/union
                                    (fields->names breakout)
                                    (mapv aggregation-column-name aggregation))
                                    (mapv aggregation-column-name aggregation))
      breakout                    (fields->names breakout)
      fields                      (fields->names fields))))

(defn- handle-fields [query {:keys [fields]}]
  (log/debug (format "driver.query-processor/handle-fields: fields=%s" fields))
  (if-not fields
    query
    ;; TODO: this will probably need to be enhanced to fully support nested fields
    (let [field-names (fields->names fields)]
      (r/pluck query field-names))))

;;; ----------------------------------------------------- breakout ---------------------------------------------------

(defn- handle-breakout-grouping [query {:keys [breakout fields]}]
  (if-not breakout
    query
    (let [breakout-fields (fields->names breakout)
          plucked-fields (set/union breakout-fields (fields->names fields))]
      (log/debug
        (format
          "driver.query-processor/handle-breakout-grouping: breakout-fields=%s plucked-field=%s"
            breakout-fields plucked-fields))
      (r/group query breakout-fields))))

(defn- breakout-fields->kv-pairs
  [breakout-fields]
  (if (= (count breakout-fields) 1)
    ;; "group" field will be string
    (cons (first breakout-fields) [(fn [row] (r/get-field row "group"))])
    ;; "group" field will be sequence
    (loop [[field & rest] breakout-fields idx 0 result '()]
      (if-not field
        result
        (recur rest (inc idx)
          (concat result [field]
            [(fn [row] (r/nth (r/get-field row "group") idx))]))))))

(defn- handle-breakout-ungrouping [query {:keys [breakout]}]
  (if-not breakout
    query
    (let [breakout-fields (fields->names breakout) 
          breakout-obj-kvs (breakout-fields->kv-pairs breakout-fields)]
      (log/debug
        (format
          "driver.query-processor/handle-breakout-ungrouping: breakout-fields=%s breakout-obj-kvs=%s"
            breakout-fields (seq breakout-obj-kvs)))
      (-> (r/ungroup query)
          (r/concat-map (r/fn [row]
            (-> (r/get-field row "reduction")
                (r/merge (r/object
                  (r/args (apply_args_to_l_r_fn_pairs breakout-obj-kvs [row])))))))))))

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

(defmethod parse-aggregation :avg [aggregation-clause idx out-column-name] 
  (let [[_ [_ in-field] _] aggregation-clause
        in-column-name (->lvalue in-field)
        sum-field-name (format "%d_sum" idx)
        count-field-name (format "%d_count" idx)]
    (vector
      ; map fields
      [ sum-field-name 
        (fn [row] (r/get-field row in-column-name))
        count-field-name
        (fn [row] 1) ]
      ; reduce fields
      [ sum-field-name
        (fn [left right] (r/add (r/get-field left sum-field-name)
                                (r/get-field right sum-field-name)))
        count-field-name
        (fn [left right] (r/add (r/get-field left count-field-name)
                                (r/get-field right count-field-name))) ]
      ; result fields
      [ out-column-name
        (fn [o] (r/div (r/get-field o sum-field-name)
                       (r/get-field o count-field-name))) ])))
  
(defmethod parse-aggregation :count [_ idx out-column-name] 
  (let [count-field-name (format "%d_count" idx)]
    (vector
      ; map fields
      [ count-field-name (fn [row] 1) ]
      ; reduce fields
      [ count-field-name
        (fn [left right] (r/add (r/get-field left count-field-name)
                                (r/get-field right count-field-name))) ]
      ; result fields
      [ out-column-name (fn [o] (r/get-field o count-field-name)) ])))

(defmethod parse-aggregation :distinct [aggregation-clause idx out-column-name] 
  (let [[_ [_ in-field] _] aggregation-clause
        in-column-name (->lvalue in-field)
        values-field-name (format "%d_values" idx)]
    (vector
      ; map fields
      [ values-field-name 
        (fn [row] (r/make-array (r/get-field row in-column-name))) ]
      ; reduce fields
      [ values-field-name
        (fn [left right] (r/append
                           (r/get-field left values-field-name)
                           (r/nth (r/get-field right values-field-name) 0))) ]
      ; result fields
      [ out-column-name
        (fn [o] (r/count (r/distinct (r/get-field o values-field-name)))) ])))

(defn- simple-aggregation-reduction
  [aggregation-clause idx out-column-name reduction-f]
  (let [[_ [_ in-field] _] aggregation-clause
        in-column-name (->lvalue in-field)
        values-field-name (format "%d_values" idx)]
    (vector
      ; map fields
      [ values-field-name 
        (fn [row] (r/get-field row in-column-name)) ]
      ; reduce fields
      [ values-field-name
        (fn [left right] (reduction-f
                           (r/make-array
                             (r/get-field left values-field-name)
                             (r/get-field right values-field-name)))) ]
      ; result fields
      [ out-column-name
        (fn [o] (r/get-field o values-field-name)) ])))

(defmethod parse-aggregation :min [aggregation-clause idx out-column-name]
  (simple-aggregation-reduction aggregation-clause idx out-column-name r/min))
    
(defmethod parse-aggregation :max [aggregation-clause idx out-column-name]
  (simple-aggregation-reduction aggregation-clause idx out-column-name r/max))

(defmethod parse-aggregation :sum [aggregation-clause idx out-column-name] 
  (let [[_ [_ in-field] _] aggregation-clause
        in-column-name (->lvalue in-field)
        sum-field-name (format "%d_sum" idx)]
    (vector
      ; map fields
      [ sum-field-name 
        (fn [row] (r/get-field row in-column-name)) ]
      ; reduce fields
      [ sum-field-name
        (fn [left right] (r/add (r/get-field left sum-field-name)
                                (r/get-field right sum-field-name))) ]
      ; result fields
      [ out-column-name
        (fn [o] (r/get-field o sum-field-name)) ] )))

(defn- collate-parsed-aggregations
  "Take parsed aggregations of the form [[map0 reduce0 result0] [map1 reduce1 result1] ..
  and return [[map0 map1 ..] [reduce0 reduce1 ..]]"
  [parsed-aggregations]
  (for [i (range 3)] (map #(nth % i) parsed-aggregations)))

(defn- make-aggregation-query-pipeline
  [[map-pairs reduce-pairs result-pairs] query]
  (-> (r/map query (r/fn [row] 
        (r/object
          (r/args
            (apply_args_to_l_r_fn_pairs map-pairs [row])))))
      (r/reduce (r/fn [left right]
        (r/object
          (r/args
            (apply_args_to_l_r_fn_pairs reduce-pairs [left right])))))
      (r/do (r/fn [o] (r/make-array o)))
      (r/map (r/fn [o]
        (r/object
          (r/args
            (apply_args_to_l_r_fn_pairs result-pairs [o])))))))

(defn- make-aggregation-pipeline-params
  [aggregations]
  (loop [[ag & next-ags] aggregations idx 0 result '()]
    (if-not ag
      (collate-parsed-aggregations result)
      (let [out-column-name (aggregation-column-name ag)
            parsed-aggregation (parse-aggregation ag idx out-column-name)]
        (log/debug
          (format
            "driver.query-processor/make-aggregation-pipeline-params: aggregation-clause=%s out-column-name=%s parsed-aggregation=%s"
            ag out-column-name parsed-aggregation))
        (recur next-ags (inc idx) (cons parsed-aggregation result))))))

(defn- handle-aggregation
  [query {aggregations :aggregation}]
  (log/debug (format "driver.query-processor/handle-aggregation: aggregations=%s" (seq aggregations)))
  (if-not (> (count aggregations) 0)
    query
    (let [return-query (-> (make-aggregation-pipeline-params aggregations)
                           (make-aggregation-query-pipeline query))]
      (log/debug
        (format
          "driver.query-processor/handle-aggregation: returning=%s"
          return-query))
          return-query)))

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
        (handle-breakout-grouping mbql-query)
        (handle-filter mbql-query)
        (handle-aggregation mbql-query)
        (handle-breakout-ungrouping mbql-query)
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
  