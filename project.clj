(defproject metabase/rethinkdb-driver "1.0.0"
  :min-lein-version "2.5.0"

  :dependencies
  [[org.clojure/core.logic "0.8.11"
    :exclusions [org.clojure/clojure]]]

  :jvm-opts
  ["-XX:+IgnoreUnrecognizedVMOptions"
   "--add-modules=java.xml.bind"]

  :profiles
  {:provided
   {:dependencies
    [[org.clojure/clojure "1.9.0"]
     [com.apa512/rethinkdb "1.0.0-SNAPSHOT"]
     [metabase-core "1.0.0-SNAPSHOT"]]}

   :uberjar
   {:auto-clean    true
    :aot :all
    :omit-source true
    :javac-options ["-target" "1.8", "-source" "1.8"]
    :target-path   "target/%s"
    :uberjar-name  "rethinkdb.metabase-driver.jar"}})