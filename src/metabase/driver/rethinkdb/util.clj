(ns metabase.driver.rethinkdb.util
  (:require [clojure.tools.logging :as log]
            [metabase
              [util :as u]]))

(defn details-to-clj-rethinkdb-params
  [{:keys [host dbname port]}]
    {:host        host
     :db          dbname
     :port        port
    })
