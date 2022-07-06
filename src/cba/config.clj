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
                      Long.))
(def BUCKET-PREFIX #(-> (get-env "BUCKET_PREFIX" ""
                          "prefix for S3 bucket names")))
(def PGURL         #(get-env "PGURL"
                      "PostgreSQL URL"))
(def SENTRY        #(get-env "SENTRY" ""
                      "Sentry DSN"))
(def FONDY-ID      #(get-env "FONDYID"
                      "Fondy merchant id"))
(def FONDY-KEY     #(get-env "FONDYKEY"
                      "Fondy merchant key"))
(def FONDY-APP-ID  #(get-env "FONDY_APP_ID"
                      "Fondy application id"))
(def FONDY-APP-KEY #(get-env "FONDY_APP_KEY"
                      "Fondy application key"))
(def SOLIDGATE-ID  #(get-env "SOLIDGATE_ID"
                      "Solidgate public key"))
(def SOLIDGATE-KEY #(get-env "SOLIDGATE_KEY"
                      "Solidgate secret key"))
(def SOLIDGATE-GO-ID  #(get-env "SOLIDGATE_GO_ID"
                      "Solidgate public key"))
(def SOLIDGATE-GO-KEY #(get-env "SOLIDGATE_GO_KEY"
                      "Solidgate secret key"))
(def TIMEOUT       #(-> (get-env "TIMEOUT" "60000"
                          "HTTP timeout")
                        str/trim
                        Long.))
