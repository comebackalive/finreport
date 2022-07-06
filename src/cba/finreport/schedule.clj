(ns cba.finreport.schedule
  (:require [clojure.tools.logging :as log]

            [cba.finreport.fondy :as fondy]
            [mount.core :as mount]
            [cba.finreport.solidgate :as solidgate]
            [cba.config :as config]))


(defn run-schedule []
  (let [stop (atom false)
        id   (format "s%x" (mod (System/currentTimeMillis)
                             1000000))
        t    (Thread.
               (fn []
                 (loop [i 0]
                   (if @stop
                     (log/infof "schedule %s: stop signal" id)

                     (let [next-inc (if (config/DEV)
                                      (inc (rand-int 150))
                                      1)]
                       (log/debugf "schedule %s (next in %ss)" id next-inc)
                       (try
                         (fondy/cron i)
                         (solidgate/cron i :charity)
                         (solidgate/cron i :go)
                         (catch Exception e
                           (log/error e "cron error")))
                       (try
                         (Thread/sleep (* 1000 next-inc))
                         (catch InterruptedException _
                           (log/infof "schedule %s: sleep interrupt" id)))
                       (recur (+ i next-inc)))))))]
    (log/infof "schedule %s: start" id)
    (.start t)
    (fn []
      (reset! stop true)
      (.interrupt t))))


(mount/defstate schedule
  :start (run-schedule)
  :stop  (schedule))
