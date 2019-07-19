(ns metabase.driver.rethinkdb.util
  (:require [clojure.tools.logging :as log]
            [metabase
              [util :as u]]
            [metabase.util
              [i18n :refer [trs]]
              [ssh :as ssh]]
            [rethinkdb
              [core :as rc]
              [query :as r]]))

(def ^:dynamic ^rethinkdb.core.Connection *rethinkdb-connection*
  "Connection to a RethinkDB database. Bound by top-level `with-rethinkdb-connection` so it may be reused
  within its body."
  nil)

(defn apply_args_to_l_r_fn_pairs
  "Take [l0 r_fn0 l1 r_fn1 ..] and transform it to the sequence (l0 r0 l1 r1 ..) by
   evaluating the sequence pairwise and calling r_fn1 with args."
   [lr_fn_pairs args]
   (loop [[l r_fn & rest] (flatten lr_fn_pairs) result []]
     (if-not l 
       result
       (recur rest (concat result [l (apply r_fn args)])))))

(defn- database->details
  "Make sure DATABASE is in a standard db details format. This is done so we can accept several different types of
   values for DATABASE, such as plain strings or the usual MB details map."
  [database]
  (cond
    (string? database)            {:dbname database}
    (:dbname (:details database)) (:details database) ; entire Database obj
    (:dbname database)            database            ; connection details map only
    :else                         (throw (Exception. (str "bad connection details:"
                                                          (:details database))))))

(defn- details-to-clj-rethinkdb-params
  [{:keys [host dbname port]}]
    {:host        host
     :db          dbname
     :port        port
    })

(defn *-to-clj-rethinkdb-params
  [maybe-details-or-database]
  (let [details (database->details maybe-details-or-database)]
    (details-to-clj-rethinkdb-params details)))

(defn -with-rethinkdb-connection
  "Run `f` with a new connection (bound to `*rethinkdb-connection*`) to `database`. Don't use this directly; use
  `with-rethinkdb-connection`."
  [f database]
  (let [details (database->details database)]
    (ssh/with-ssh-tunnel [details-with-tunnel details]
      (let [clj-rethinkdb-params (*-to-clj-rethinkdb-params details-with-tunnel)
           rethinkdb-conn (apply r/connect (mapcat identity clj-rethinkdb-params))]
       (log/debug (u/format-color 'cyan (trs "Opened new RethinkDB connection.")))
       (try
         (binding [*rethinkdb-connection* rethinkdb-conn]
           (f *rethinkdb-connection*))
         (finally
           (rc/close rethinkdb-conn)
           (log/debug (u/format-color 'cyan (trs "Closed RethinkDB connection.")))))))))

(defmacro with-rethinkdb-connection
  "Open a new RethinkDB connection to `database-or-details-map`, bind connection to `binding`, execute `body`, and
  close the connection. The DB connection is re-used by subsequent calls to `with-rethinkdb-connection` within
  `body`.

   DATABASE-OR-CONNECTION-STRING can also optionally be the connection details map on its own."
  [[binding database-or-details-map] & body]
  `(let [f# (fn [~binding]
              ~@body)]
     (if *rethinkdb-connection*
       (f# *rethinkdb-connection*)
       (-with-rethinkdb-connection f# ~database-or-details-map))))
    