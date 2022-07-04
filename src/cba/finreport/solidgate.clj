(ns cba.finreport.solidgate
  (:import [java.time LocalDate LocalDateTime]
           [java.time.format DateTimeFormatter]
           [javax.crypto Mac]
           [javax.crypto.spec SecretKeySpec])
  (:require [clojure.string :as str]
            [ring.util.codec :as codec]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [next.jdbc :as jdbc]
            [next.jdbc.sql :as sql]

            [cba.config :as config]
            [cba.core :as core]
            [cba.finreport.process :as process]
            [cba.finreport.db :as db]))


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
     :date    (process/gmt (process/dt dt-fmt (:created_at order)))
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


(defn mkrow [fname row]
  [(:id row) fname (:bank row) (:date row) (:amount row) (:comment row)
   (db/string-array (:tags row))
   (db/string-array (:hiddens row))])


(def UPSERT
  "ON CONFLICT (order_id)
   DO UPDATE SET amount = EXCLUDED.amount, comment = EXCLUDED.comment
   RETURNING (xmax = 0) AS inserted")


(defn write-db [fname rows]
  (jdbc/with-transaction [tx db/conn]
    (let [fields (into [:order_id] (:donate process/FIELDS))
          mkrow  (partial mkrow fname)
          insres (sql/insert-multi! tx :report fields (map mkrow rows)
                   {:batch      true
                    :batch-size 1000
                    :suffix     UPSERT})]
      {:delete   (count (remove :inserted insres))
       :inserted (count insres)})))


(defn store! [from to]
  (let [res (get-report {:from from :to to})]
    (write-db "solidgate api" (map report->row res))))


(defn cron []
  (let [dt (LocalDateTime/now)
        d  (LocalDate/now)]
    (if (= 1 (.getHour dt))
      ;; update old data in the time of downtime
      (store!
        (str (.minusDays d 3) " 00:00:00")
        (str (.plusDays d 1) " 00:00:00"))
      (store!
        (str d " 00:00:00")
        (str (.plusDays d 1) " 00:00:00")))))


(comment
  (doseq [i (range 1 20)]
    (store!
      (format "2022-06-%02d 00:00:00" i)
      (format "2022-06-%02d 00:00:00" (inc i)) ))
  (store! "2022-06-17 00:00:00" "2022-06-18 00:00:00")
  (store! "2022-06-18 00:00:00" "2022-06-19 00:00:00")
  (def q (get-report {:from "2022-06-17 00:00:00"
                      :to   "2022-06-18 00:00:00"})))
