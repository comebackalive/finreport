(ns cba.finreport.httpd
  (:require [org.httpkit.server :as httpd]
            [clojure.java.io :as io]
            [clojure.tools.logging :as log]
            [clojure.stacktrace :as st]
            [mount.core :as mount]
            [hiccup2.core :as hi]
            [hiccup.page :refer [doctype]]
            [ring.util.response :as response]
            [ring.middleware.content-type :as ctype]
            [ring.middleware.params :as params]
            [ring.middleware.multipart-params :as multipart]
            [cognitect.aws.client.api :as aws]
            [sentry-clj.ring :as sentry]

            [cba.config :as config]
            [cba.core :as core]
            [cba.finreport.process :as process]
            [ring.util.codec :as codec]))


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
          [:label "Введіть ім'я файлу або його початок"
           [:input {:type "text" :name "fname"}]]
          [:br]
          [:input {:type "submit" :value "Запуск"}]]

         [:script {:src          "static/twinspark.js"
                   :data-timeout "600000"}]
         [:script {:src "static/script.js"}]]]))})



(defn track-process [f & [cb]]
  (let [start  (System/currentTimeMillis)
        res    (process/process-and-store :db (:filename f) (:tempfile f))
        cb-res (when cb (cb (:filename f) (:tempfile f) res))
        total  (- (System/currentTimeMillis) start)]
    (hi/html
      [:div
       "Банк " (name (:bank res)) ", "
       "файл " [:a {:href (str "download/" (:filename f))} (:filename f)]
       (format "(%s), " (core/human-bytes (:size f)))
       (format "збережено в архіві: %s, зайняло %sмс"
         cb-res
         total)
       [:details
        [:summary
         (format "Завантажено %s рядків, було в базі %s, пропущені (%s):"
           (:inserted res)
           (:deleted res)
           (count (:skipped res)))]
        [:table.skipped
         (for [row (:skipped res)]
           [:tr (for [cell row]
                  [:td cell])])]]])))


(defn s3-archive [fname file res]
  (let [ba   (-> file io/input-stream .readAllBytes)
        path (str (name (:bank res)) "/" (process/file-name fname))]
    (-> (s3 :PutObject {:Bucket ARCHIVE
                        :Key    path
                        :Body   ba})
        :ETag
        boolean)))


(defn upload [req]
  (try
    (let [f    (get-in req [:multipart-params "file"])
          info (track-process f
                 (when-not (config/DEV) s3-archive))]
      {:status  200
       :headers {"Content-Type" "text/html"}
       :body    (str info)})
    (catch Exception e
      (if (ex-data e)
        (do
          (st/print-stack-trace e)
          {:status  400
           :headers {"Content-Type" "text/html"}
           :body    (str "<pre>"
                      (.getMessage e) ": " (pr-str (ex-data e))
                      "</pre>")})
        (throw e)))))


(defn download [req]
  (let [path (-> req :path-params :path codec/url-decode)
        obj  (s3 :GetObject {:Bucket ARCHIVE
                             :Key    path})]
    (if (:Error obj)
      {:status  404
       :headers {"Content-Type" "text/plain"}
       :body    "Not Found"}
      {:status  200
       :headers {"Content-Disposition" (format "attachment; filename=\"%s\""
                                         (process/file-name path))
                 "Content-Length"      (:ContentLength obj)}
       :body    (:Body obj)})))


(defn reprocess [req]
  (if-let [fname (-> req :params (get "fname") not-empty)]
    (let [fnames (when (not-empty fname)
                   (->> (:Contents
                         (s3 :ListObjectsV2 {:Bucket ARCHIVE
                                             :Prefix fname}))
                        (map :Key)))
          _      (log/info "Reprocessing" fnames)
          res    (into []
                   (for [fname fnames
                         :let  [data (s3 :GetObject {:Bucket ARCHIVE
                                                     :Key    fname})]]
                     (track-process {:filename fname
                                     :tempfile (:Body data)
                                     :size     (:ContentLength data)})))]
      {:status  200
       :headers {"Content-Type" "text/html"}
       :body    (str
                  (hi/html
                    [:div
                     (for [info res]
                       [:p {} info])]))})
    {:status  400
     :headers {"Content-Type" "text/plain"}
     :body    "Please supply file name"}))


(defn reprocess-all [& [{:keys [check fs]}]]
  (let [check (or check identity)
        fs    (or fs (->> (:Contents
                           (s3 :ListObjectsV2 {:Bucket ARCHIVE}))
                          (mapv :Key)))]
    (println "In total" (count fs))
    (doseq [[i f] (map-indexed vector fs)
            :let [body (:Body (s3 :GetObject {:Bucket ARCHIVE :Key f}))]]
      (println i f)
      (if (check f)
        (println (-> (process/process-and-store :db f body)
                     (update :skipped count)))
        (println "skipped")))))


;;; Routing/app

(defn mk-handler [req func & names]
  (fn [m]
    (func (assoc req :path-params (zipmap names (rest m))))))


(defn router [req]
  (or (condp re-find (:uri req)
        #"^/static/(.*)"   :>> (mk-handler req static :path)
        #"^/upload"        :>> (mk-handler req upload)
        #"^/download/(.*)" :>> (mk-handler req download :path)
        #"^/reprocess"     :>> (mk-handler req reprocess)
        #"^/$"             :>> (mk-handler req index)
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


(mount/defstate httpd
  :start (start)
  :stop (httpd))
