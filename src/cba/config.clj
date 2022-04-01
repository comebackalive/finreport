(ns cba.config
  (:require [clojure.string :as str]))


(set! *warn-on-reflection* true)


(defn get-env
  ([var-name desc]
   (get-env var-name nil desc))
  ([var-name default desc]
   (or (some-> (System/getenv var-name) str/trim)
       default
       (do
         (. (Runtime/getRuntime) addShutdownHook
           (Thread. #(binding [*out* *err*]
                       (println (str var-name " - " desc " (required)")))))
         (System/exit 1)))))


(def DEV           #(-> (get-env "ENV" ""
                          "set an app environment, dev/prod")
                      (not= "prod")))
(def PORT          #(-> (get-env "PORT" "2323"
                          "port to start on")
                      str/trim
                      Integer/parseInt))
(def BUCKET-PREFIX #(-> (get-env "BUCKET_PREFIX" ""
                          "prefix for S3 bucket names")))
(def PGURL         #(get-env "PGURL"
                      "PostgreSQL URL"))
(def SENTRY        #(get-env "SENTRY" ""
                      "Sentry DSN"))

