(ns cba.core
  (:refer-clojure :exclude [bytes])
  (:import [java.security MessageDigest]
           [org.apache.commons.codec.binary Hex])
  (:require [clojure.stacktrace :as st]
            [org.httpkit.client :as http]
            [cheshire.core :as json]
            [cba.config :as config]))


(set! *warn-on-reflection* true)


(def ^MessageDigest sha1md (MessageDigest/getInstance "SHA-1"))
(def ^MessageDigest sha512md (MessageDigest/getInstance "SHA-512"))


(defn bytes ^bytes [^String s]
  (.getBytes s "UTF-8"))


(defn hex [^bytes v]
  (Hex/encodeHexString v))


(defn sha1 [^String v]
  (hex (.digest sha1md (bytes v))))


(defn sha512 [^String v]
  (hex (.digest sha512md (bytes v))))


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


(defn post!
  ([url] (post! url nil))
  ([url body]
   (let [res  @(http/request {:method  :post
                              :url     url
                              :headers {"Content-Type" "application/json"}
                              :body    (when body (json/encode body))
                              :timeout (config/TIMEOUT)})
         data (-> res :body (json/parse-string true))]
     (with-meta (or data {}) {:original res}))))


;; taken from `medley`
(defn distinct-by
  "Returns a lazy sequence of the elements of coll, removing any elements that
  return duplicate values when passed to a function f. Returns a transducer
  when no collection is provided."
  ([f]
   (fn [rf]
     (let [seen (volatile! #{})]
       (fn
         ([] (rf))
         ([result] (rf result))
         ([result x]
          (let [fx (f x)]
            (if (contains? @seen fx)
              result
              (do (vswap! seen conj fx)
                  (rf result x)))))))))
  ([f coll]
   (let [step (fn step [xs seen]
                (lazy-seq
                 ((fn [[x :as xs] seen]
                    (when-let [s (seq xs)]
                      (let [fx (f x)]
                        (if (contains? seen fx)
                          (recur (rest s) seen)
                          (cons x (step (rest s) (conj seen fx)))))))
                  xs seen)))]
     (step coll #{}))))
