(ns cba.finreport.httpd
  (:require [org.httpkit.server :as httpd]
            [clojure.stacktrace :as st]
            [hiccup2.core :as hi]
            [hiccup.page :refer [doctype]]
            [ring.util.response :as response]
            [ring.middleware.content-type :as ctype]
            [ring.middleware.multipart-params :as multipart]
            [cognitect.aws.client.api :as aws]
            [sentry-clj.ring :as sentry]

            [cba.config :as config]
            [cba.finreport.process :as process]
            [clojure.java.io :as io]))


(def s3 (aws/client {:api    :s3
                     :region "us-east-1"}))



(defn exc-mw [handler]
  (fn [req]
    (try (handler req)
         (catch Exception e
           {:status  500
            :headers {"Content-Type" "text/plain"}
            :body    (with-out-str (st/print-stack-trace e))}))))


(defn static [{{:keys [path]} :path-params}]
  (-> (response/resource-response path {:root "public"})
      #_(assoc-in [:headers "Cache-Control"] "max-age=3600")))


(defn index [req]
  {:status  200
   :headers {"Content-Type" "text/html"}
   :body
   (str
     (hi/html
       (:html5 doctype)
       [:html
        [:head
         [:meta {:charset "utf-8"}]
         [:link {:rel "icon" :href "data:;base64,="}]
         [:link {:rel "stylesheet" :href "static/style.css"}]
         [:title "Upload Financial Reports"]]
        [:body
         [:div.dropzone
          {:ondrop      "prevent(event); drop(event)"
           :ondragover  "prevent(event)"
           :ondragenter "dragStart(event)"
           :ondragleave "dragStop(event)"}
          [:h1 "Drop them all here"]
          [:ul#files]]
         [:script {:src "static/script.js"}]]]))})


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


(defn upload [req]
  (try
    (let [start (System/currentTimeMillis)
          f     (get-in req [:multipart-params "file"])
          cnt   (->> (process/process (:filename f) (:tempfile f))
                     (process/write-db (:filename f)))
          res   (let [ba (-> (:tempfile f) io/input-stream .readAllBytes)]
                  (aws/invoke s3
                    {:op      :PutObject
                     :request {:Bucket "cba-finreport-archive"
                               :Key    (process/file-name (:filename f))
                               :Body   ba}}))
          total (- (System/currentTimeMillis) start)]
      {:status 200
       :body   (format
                 "Size: %s, rows with data: %s (was %s), archived: %s, took %sms"
                 (human-bytes (:size f))
                 (:inserted cnt)
                 (:deleted cnt)
                 (boolean (:ETag res))
                 total)})
    (catch Exception e
      (if (ex-data e)
        {:status 400
         :body   (str (.getMessage e) ": " (pr-str (ex-data e)))}
        (throw e)))))


(defn mk-handler [req func & names]
  (fn [m]
    (func (assoc req :path-params (zipmap names (rest m))))))


(defn router [req]
  (or (condp re-find (:uri req)
        #"^/static/(.*)" :>> (mk-handler req static :path)
        #"^/upload"      :>> (mk-handler req upload)
        #"^/$"           :>> (mk-handler req index)
        nil)
      {:status  404
       :headers {"Content-Type" "text/plain"}
       :body    "Not Found"}))


(defn -app [req]
  (router req))


(defn make-app []
  (cond-> -app
    true            (ctype/wrap-content-type)
    true            (multipart/wrap-multipart-params)
    (config/DEV)    exc-mw
    (config/SENTRY) (sentry/wrap-report-exceptions nil)))


(defn start []
  (let [port (config/PORT)]
    (println (str "Listening to http://127.0.0.1:" port))
    (httpd/run-server (make-app) {:port port})))
