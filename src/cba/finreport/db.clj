(ns cba.finreport.db
  (:import [java.net URI]
           [java.sql Array PreparedStatement]
           [org.postgresql.ds PGSimpleDataSource])
  (:require [clojure.string :as str]
            [mount.core :as mount]
            [ring.util.codec :as codec]
            [next.jdbc :as jdbc]
            [next.jdbc.prepare :as jdbc-p]
            [next.jdbc.result-set :as jdbc-rs]
            [next.jdbc.date-time :as jdbc-dt]
            [ragtime.jdbc]
            [ragtime.repl]
            [ragtime.strategy]

            [cba.config :as config]))


(set! *warn-on-reflection* true)
(jdbc-dt/read-as-instant)


(defn set-pg-opts [^PGSimpleDataSource ds opts]
  (doseq [[k v] opts]
    (case k
      "sslmode"     (.setSslMode ds v)
      "sslrootcert" (.setSslRootCert ds v)
      "options"     (.setOptions ds v))))


(defn make-conn [url]
  (let [uri        (URI. url)
        [user pwd] (some-> (.getUserInfo uri) (str/split #":"))
        port       (if (= -1 (.getPort uri))
                     5432
                     (.getPort uri))
        opts       (some-> (.getQuery uri) codec/form-decode)]
    (doto (PGSimpleDataSource.)
      (.setServerName (.getHost uri))
      (.setPortNumber port)
      (.setDatabaseName (.substring (.getPath uri) 1))
      (.setUser user)
      (.setPassword pwd)
      (.setPrepareThreshold 0)
      (set-pg-opts opts))))


(defn ragtime-config [db]
  {:datastore  (ragtime.jdbc/sql-database db
                 {:migration-table-name "ragtime_migrations"})
   :migrations (ragtime.jdbc/load-resources "migrations")
   :strategy   ragtime.strategy/apply-new})


(mount/defstate conn
  :start (let [conn (make-conn (config/PGURL))]
           (doto (ragtime-config {:datasource conn})
             (ragtime.repl/migrate))
           conn)
  ;:stop (.close conn)
  )


(defn q
  ([query] (q conn query))
  ([conn query]
   (jdbc/execute! conn query
     {:builder-fn jdbc-rs/as-unqualified-lower-maps})))


(defn one
  ([query] (first (q query)))
  ([conn query] (first (q conn query))))


;;; Extensions

(extend-protocol jdbc-rs/ReadableColumn
  Array
  (read-column-by-label [^Array v _]    (vec (.getArray v)))
  (read-column-by-index [^Array v _ _]  (vec (.getArray v))))


(extend-protocol jdbc-p/SettableParameter
  clojure.lang.PersistentVector
  (set-parameter [v ^PreparedStatement s ^long i]
    (.setArray s i v)))
