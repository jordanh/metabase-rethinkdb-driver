(ns metabase.driver.rethinkdb.util
  (:require [clojure.tools.logging :as log]
            [metabase
              [util :as u]]))

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
