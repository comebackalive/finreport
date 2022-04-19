(ns cba.finreport.httpd
  (:require [org.httpkit.server :as httpd]
            [clojure.java.io :as io]
            [hiccup2.core :as hi]
            [hiccup.page :refer [doctype]]
            [ring.util.response :as response]
            [ring.middleware.content-type :as ctype]
            [ring.middleware.params :as params]
            [ring.middleware.multipart-params :as multipart]
            [cognitect.aws.client.api :as aws]
            [sentry-clj.ring :as sentry]

            [cba.config :as config]
            [cba.finreport.process :as process]
            [cba.finreport.core :as core]
            [clojure.string :as str]
            [clojure.tools.logging :as log]))


(def *s3 (aws/client {:api    :s3
                      :region "us-east-1"}))
(def ARCHIVE "cba-finreport-archive")

(defn s3 [op request]
  (aws/invoke *s3 {:op op :request request}))


;;; Handlers

(defn static [{{:keys [path]} :path-params}]
  (-> (response/resource-response path {:root "public"})
      #_(assoc-in [:headers "Cache-Control"] "max-age=3600")))


(defn index [_req]
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
         [:title "Завантаження фінансових звітів"]]
        [:body
         [:div.dropzone
          {:ondrop      "prevent(event); drop(event)"
           :ondragover  "prevent(event)"
           :ondragenter "dragStart(event)"
           :ondragleave "dragStop(event)"}
          [:h1 "Скидайте їх прямо сюди"]
          [:ul#files]]

         [:h2 "Заново переобробити файли"]
         [:form {:method  "POST"
                 :action  "reprocess"
                 :ts-req  ""
                 :ts-swap "afterend"}
          [:label "Введіть початок імені файлів або декілька імен (повністю) через пробіл"
           [:input {:type "text" :name "fname"}]]
          [:br]
          [:input {:type "submit" :value "Запуск"}]]

         [:script {:src          "static/twinspark.js"
                   :data-timeout "600000"}]
         [:script {:src "static/script.js"}]]]))})



(defn track-process [f cb]
  (let [start (System/currentTimeMillis)
        cnt   (process/process-and-store :db (:filename f) (:tempfile f))
        res   (cb (:filename f) (:tempfile f))
        total (- (System/currentTimeMillis) start)]
    (format
      "Size: %s, rows with data: %s (was %s), archived: %s, took %sms"
      (core/human-bytes (:size f))
      (:inserted cnt)
      (:deleted cnt)
      res
      total)))


(defn upload [req]
  (try
    (let [f    (get-in req [:multipart-params "file"])
          info (track-process f
                 (fn [fname file]
                   (let [ba (-> file io/input-stream .readAllBytes)]
                     (-> (s3 :PutObject {:Bucket ARCHIVE
                                         :Key    (process/file-name fname)
                                         :Body   ba})
                         :ETag
                         boolean))))]
      {:status  200
       :headers {"Content-Type" "text/plain"}
       :body    info})
    (catch Exception e
      (if (ex-data e)
        {:status  400
         :headers {"Content-Type" "text/plain"}
         :body    (str (.getMessage e) ": " (pr-str (ex-data e)))}
        (throw e)))))


(defn reprocess [req]
  (let [fnames (-> req :params (get "fname" "") (str/split #" "))
        fnames (cond
                 (= fnames [""]) []

                 (= (count fnames) 1)
                 (->> (:Contents
                       (s3 :ListObjectsV2 {:Bucket ARCHIVE
                                           :Prefix (first fnames)}))
                      (map :Key))

                 :else fnames)
        _   (log/info "Reprocessing" fnames)
        res (into []
              (for [fname fnames
                    :let  [data (s3 :GetObject {:Bucket ARCHIVE
                                                :Key    fname})]]
                (track-process {:filename fname
                                :tempfile (:Body data)
                                :size     (:ContentLength data)}
                  (constantly false))))]
    (prn res)
    {:status  200
     :headers {"Content-Type" "text/html"}
     :body    (str
                (hi/html
                  [:div
                   (for [info res]
                     [:p {} info])]))}))


(defn manual [req]
  nil)


;;; Routing/app

(defn mk-handler [req func & names]
  (fn [m]
    (func (assoc req :path-params (zipmap names (rest m))))))


(defn router [req]
  (or (condp re-find (:uri req)
        #"^/static/(.*)" :>> (mk-handler req static :path)
        #"^/upload"      :>> (mk-handler req upload)
        #"^/reprocess"   :>> (mk-handler req reprocess)
        #"^/manual"      :>> (mk-handler req manual)
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
    true            (params/wrap-params)
    true            (multipart/wrap-multipart-params)
    (config/DEV)    core/exc-mw
    (config/SENTRY) (sentry/wrap-report-exceptions nil)))


(defn start []
  (let [port (config/PORT)]
    (println (str "Listening to http://127.0.0.1:" port))
    (httpd/run-server (make-app) {:port     port
                                  :max-body (* 30 1024 1024)})))
