(ns cba.finreport.fondy
  (:import [java.time Instant LocalDate])
  (:require [cba.config :as config]
            [clojure.string :as str]
            [cba.core :as core]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [clojure.tools.logging :as log]
            [cba.finreport.process :as process]))


(set! *warn-on-reflection* true)


(def BASE "https://portal.fondy.eu/api")
(defn url [path] (str BASE path))


(defn sign [ctx]
  (let [s      (->> (sort ctx)
                    (map val)
                    (remove (comp empty? str))
                    (str/join \|))
        full-s (str (config/FONDY-KEY) "|" s)]
    (assoc ctx :signature (core/sha1 full-s))))


(defn sign-app [ctx]
  (let [s      (->> (sort ctx)
                    (map val)
                    (remove (comp empty? str))
                    (str/join \|))
        full-s (str (config/FONDY-APP-KEY) "|" s)]
    (assoc ctx :signature (core/sha512 full-s))))


(defn verify! [ctx]
  (let [signed (sign (dissoc ctx :signature :response_signature_string))]
    (if (= (:signature signed) (:signature ctx))
      ctx
      (throw (ex-info "Bad signature, check credentials"
               {:theirs             (:signature ctx)
                :ours               (:signature sign)
                ::invalid-signature true})))))


(defn req!
  ([url ctx] (req! url nil ctx))
  ([url {:keys [signer token]} ctx]
   (let [ctx  (if signer
                (signer ctx)
                (sign ctx))
         res  @(http/request
                 {:method  :post
                  :url     (if (str/starts-with? url "https:")
                             url
                             (str BASE url))
                  :headers (cond-> {"Content-Type" "application/json"}
                             token
                             (assoc "Authorization" (str "Token " token)))
                  :body    (json/encode ctx)
                  :timeout (config/TIMEOUT)})
         data (-> res :body (json/parse-string true))]
     (log/debugf "req %s %s %s" (:status res) url ctx)
     (if (:err_code data)
       (throw (ex-info (:error data) {:data data}))
       data))))


(defn get-token! []
  (-> (req! "https://wallet.fondy.eu/authorizer/token/application/get"
        {:signer sign-app}
        {:application_id (config/FONDY-APP-ID)
         :date           (str (Instant/now))})
      :token))


(defn report->row [src]
  (let [data (process/parse-card-oid (:order_id src))]
    {:id      (:order_id src)
     :bank    (if (:sub data)
                "Subscription"
                "Card")
     :date    (process/dt (:order_timestart src))
     :amount  (or (process/parse-n (:settlement_amount src))
                  ;; embed commission, newer tx dont have settlement_amount yet
                  (* 0.979 (process/parse-n (:actual_amount src))))
     :comment (format "%s ***%s (%s %s)"
                (:order_desc src)
                (last (str/split (:masked_card src) #"\*"))
                (:amount src)
                (:currency src))
     :tags    (:tags data)
     :hiddens (:hiddens data)}))


(defn get-report [{:keys [from to onpage]
                   :or   {onpage 500}}]
  (let [token (get-token!)
        res   (iteration
                (fn [page]
                  (req! "/extend/company/report/" {:token token}
                    {:filters
                     [{:s "order_timestart" :m "from" :v from}
                      {:s "order_timestart" :m "to"   :v to}
                      ;; handle "reversed" if necessary
                      {:s "order_status"    :m "eq"   :v "approved"}]
                     :merchant_id (config/FONDY-ID)
                     :report_id   745
                     :on_page     onpage
                     :page        page}))
                {:initk 1
                 :kf    (fn [res]
                          (let [curr (* (:rows_page res)
                                        (:rows_on_page res))]
                            (when (< curr (:rows_count res))
                              (inc (:rows_page res)))))
                 :vf    (fn [res]
                          (let [fields (mapv keyword (:fields res))]
                            (map #(zipmap fields %) (:data res))))})]
    (into [] cat res)))


(defn cron []
  (let [d   (LocalDate/now)
        res (get-report {:from (str (.minusDays d 1))
                         :to   (str d)})]
    (process/write-db "fondy api" (map report->row res))))


(comment
  (def q (get-report {:from "2022-04-27" :to "2022-04-27"}))

  (process/write-db "fondy api" q))
