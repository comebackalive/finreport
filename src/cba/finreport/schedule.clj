(ns cba.finreport.schedule
  (:require [clojure.tools.logging :as log]

            [cba.finreport.fondy :as fondy]
            [mount.core :as mount]
            [cba.finreport.solidgate :as solidgate]))


(defn run-schedule []
  (let [stop (atom false)
        id   (format "s%x" (mod (System/currentTimeMillis)
                             1000000))
        t    (Thread.
               (fn []
                 (if @stop
                   (log/infof "schedule %s: stop signal" id)

                   (do
                     (log/debugf "schedule %s" id)
                     (try
                       (fondy/cron)
                       (solidgate/cron)
                       (catch Exception e
                         (log/error e "cron error")))
                     (try
                       (Thread/sleep (* 5 60 1000))
                       (catch InterruptedException _
                         (log/infof "schedule %s: sleep interrupt" id)))
                     (recur)))))]
    (log/infof "schedule %s: start" id)
    (.start t)
    (fn []
      (reset! stop true)
      (.interrupt t))))


(mount/defstate schedule
  :start (run-schedule)
  :stop  (schedule))
