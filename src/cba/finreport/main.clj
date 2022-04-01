(ns cba.finreport.main
  (:gen-class)
  (:require [clojure.java.io :as io]
            [mount.core :as mount]
            [org.httpkit.client :as http]
            [org.httpkit.sni-client :as sni-client]
            [sentry-clj.core :as sentry]

            [cba.config :as config]
            [cba.finreport.httpd :as httpd]))


(set! *warn-on-reflection* true)
(alter-var-root #'http/*default-client* (fn [_] sni-client/default-client))


(def VERSION
  (or (some-> (io/resource "VERSION") slurp)
      "dev"))


(when (seq (config/SENTRY))
  (sentry/init! (config/SENTRY) {:release VERSION
                                 :in-app-includes ["uapatron"]}))

(mount/defstate httpd
  :start (httpd/start)
  :stop (httpd))


(defn -main [& args]
  (case (first args)
    nil  (mount/start)
    "-V" (println "cba-finreport " VERSION)))
