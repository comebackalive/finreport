(ns cba.finreport.core
  (:require [clojure.stacktrace :as st]))


(defn exc-mw [handler]
  (fn [req]
    (try (handler req)
         (catch Exception e
           (let [st (with-out-str (st/print-stack-trace e))]
             (println st)
             {:status  500
              :headers {"Content-Type" "text/plain"}
              :body    st})))))


(defn human-bytes
  "https://stackoverflow.com/questions/3758606#answer-24805871"
  [^long v]
  (if (< v 1024)
    (str v "B")
    (let [z (-> (- 63 (Long/numberOfLeadingZeros v))
                (/ 10))]
      (format "%.1f%sB"
        (/ (double v) (bit-shift-left 1 (* (int z) 10)))
        (.charAt " KMGTPE" z)))))
