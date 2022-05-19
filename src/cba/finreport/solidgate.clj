(ns cba.finreport.solidgate
  (:import [java.time LocalDateTime]
           [java.time.format DateTimeFormatter]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec])
  (:require [clojure.string :as str]
            [ring.util.codec :as codec]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]

            [cba.config :as config]
            [cba.core :as core]
            [cba.finreport.process :as process]))


(set! *warn-on-reflection* true)
(def BASE "https://reports.solidgate.com/api/v2")
(def dt-fmt (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))


(defn hmac-sha512 [^String key ^String s]
  (let [hmac (Mac/getInstance "HmacSHA512")
        spec (SecretKeySpec. (.getBytes key "UTF-8") "HmacSHA512")]
    (.init hmac spec)
    (.doFinal hmac (.getBytes s "UTF-8"))))


(defn sign [ctx-str]
  (-> (hmac-sha512 (config/SOLIDGATE-KEY)
        (str (config/SOLIDGATE-ID) ctx-str (config/SOLIDGATE-ID)))
      core/hex
      core/bytes
      codec/base64-encode))


(defn req! [url ctx]
  (let [json (json/generate-string ctx)
        res  @(http/request
                {:method  :post
                 :url     (if (str/starts-with? url "https:")
                            url
                            (str BASE url))
                 :headers {"Merchant"  (config/SOLIDGATE-ID)
                           "Signature" (sign json)}
                 :body    json
                 :timeout (config/TIMEOUT)})
        data (-> res :body (json/parse-string true))]
    (log/debugf "req %s %s %s" (:status res) url ctx)
    (if (:error data)
      (throw (ex-info (-> data :error :messages first) {:data data}))
      (with-meta data {:response res}))))


(defn get-report [{:keys [from to]}]
  (let [res (iteration
              (fn [cursor]
                (req! "/reconciliation/orders"
                  {:date_from          from
                   :date_to            to
                   :next_page_iterator cursor}))
              {:initk nil
               :kf    (fn [res] (-> res :metadata :next_page_iterator))
               :vf    (fn [res] (:orders res))})
        xf (comp
             cat
             (filter #(= "approved" (:status %))))]
    (into [] xf res)))


(defn parse-source [src]
  (reduce
    (fn [acc v]
      (if (str/starts-with? v "_")
        (update acc :hiddens conj (subs v 1))
        (update acc :tags conj v)))
    {:tags    []
     :hiddens []}
    (str/split src #"~")))


(defn report->row [order]
  (let [tx   (-> order :transactions first)
        data (some-> (:traffic_source order) parse-source)]
    {:id      (:order_id order)
     :bank    (if (= (:type order) "recurring")
                "Subscription"
                "Card")
     :date    (process/dt dt-fmt (:created_at order))
     :amount  (if (= (:processing_currency order) "UAH")
                (/ (:processing_amount order) 100.0)
                (throw (ex-info "Can't process tx currency"
                         {:processing_currency (:processing_currency order)})))
     :comment (format "%s ***%s (%s %s)"
                (-> tx :card :country)
                (last (str/split (-> tx :card :number) #"XXX"))
                (process/fmt-amount (/ (:amount order) 100.0))
                (:currency order))
     :tags    (:tags data)
     :hiddens (:hiddens data)}))


(defn cron []
  (let [d   (LocalDateTime/now)
        res (get-report {:from (.format (.minusDays d 1) dt-fmt)
                         :to   (.format d dt-fmt)})]
    (process/write-db "solidgate api" (map report->row res))))


(comment
  (req! "/reconciliation/orders"
    {:date_from "2022-05-18 00:00:00"
     :date_to   "2022-05-19 23:59:59"}))
