(ns metabase.driver.rethinkdb.util
  (:require [clojure.tools.logging :as log]
            [metabase
              [util :as u]]))

; (import com.rethinkdb.RethinkDB)
; (def r com.rethinkdb.RethinkDB/r)

; (defn- details-to-rethinkdb-connection-params
;   [{:keys [host dbname port]}]
;     {:host        host
;      :db          dbname
;      :port        port
;     })


; (defn connect
;   [details]
;   (let  [connect-params (details-to-rethinkdb-connection-params details)
;          {:keys [dbname host port]} connect-params]
;     (-> (.connection r)
;         (cond->
;           (not-empty dbname) (.dbname dbname)
;           (not-empty host) (.hostname host)
;           (not-empty port) (.port port))
;         .connect)))
