(ns cba.finreport.solidgate
  (:import [java.time LocalDate]
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
(def BASE "https://reports.solidgate.com/api")
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


(defn get-report
  "Solidgate docs: https://dev.solidgate.com/developers/documentation/reports/"
  [{:keys [from to]}]
  (let [res (iteration
              (fn [cursor]
                (req! "/v1/card-orders"
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
      (cond
        (str/starts-with? v "_") (update acc :hiddens conj (subs v 1))
        (str/starts-with? v "#") (update acc :tags conj (subs v 1))
        :else                    (update acc :tags conj v)))
    {:tags    []
     :hiddens []}
    (str/split src #"~")))


(defn to-uah [currency amount]
  (case currency
    nil   (or amount 0)
    "UAH" (/ amount 100.0)
    "USD" (* amount 0.2925)
    "EUR" (* amount 0.3161)
    (throw (ex-info (str "Can't process tx currency " currency)
             {:currency currency}))))


(defn report->row [order]
  (let [tx   (-> order :transactions first)
        data (merge-with into
               (process/parse-card-oid (:order_id order))
               (some-> (:traffic_source order) parse-source))]
    {:id      (:order_id order)
     :bank    (if (= (:type order) "recurring")
                "Solidgate Sub"
                "Solidgate Card")
     :date    (process/dt dt-fmt (:created_at order))
     :amount  (- (to-uah
                   (:processing_currency order) (:processing_amount order))
                 (to-uah
                   (:finance_fee_currency tx) (:finance_fee_amount tx)))
     :comment (format "%s ***%s (%s %s)"
                (-> tx :card :country)
                (last (str/split (-> tx :card :number) #"XXX"))
                (process/fmt-amount (/ (:amount order) 100.0))
                (:currency order))
     :tags    (:tags data)
     :hiddens (:hiddens data)}))


(defn store! [from to]
  (let [res (get-report {:from from :to to})]
    (process/write-db "solidgate api" (map report->row res))))


(defn cron []
  (let [d (LocalDate/now)]
    (store!
      (str (.minusDays d 1) " 00:00:00")
      (str (.plusDays d 1) " 00:00:00"))))


(comment
  (def q (get-report {:from "2022-06-17 00:00:00"
                     :to   "2022-06-18 00:00:00"})))
