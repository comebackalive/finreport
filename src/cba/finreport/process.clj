(ns cba.finreport.process
  (:import [java.util Date]
           [java.time LocalDateTime LocalDate ZoneId]
           [java.time.format DateTimeFormatter DateTimeParseException]
           [org.apache.poi.ss.usermodel WorkbookFactory Cell CellType DateUtil])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.csv :as csv]
            [clojure.tools.logging :as log]
            [next.jdbc.sql :as sql]

            [cba.finreport.db :as db]
            [next.jdbc :as jdbc]))


(set! *warn-on-reflection* true)

;;; Parsing

(def dt-fmt (DateTimeFormatter/ofPattern "dd.MM.yyyy HH:mm:ss"))
(def fondy-dt-fmt (DateTimeFormatter/ofPattern "dd.MM.yy HH:mm:ss"))
(def date-fmt (DateTimeFormatter/ofPattern "dd.MM.yyyy"))

(defn dt [s]
  (cond
    (string? s)             (try (LocalDateTime/parse s dt-fmt)
                                 (catch DateTimeParseException _
                                   (LocalDateTime/parse s fondy-dt-fmt)))
    (instance? LocalDate s) (.atStartOfDay ^LocalDate s)
    :else                   s))

(defn date [s]
  (cond
    (string? s)                 (LocalDate/parse s date-fmt)
    (instance? LocalDateTime s) (.toLocalDate ^LocalDateTime s)
    (instance? Date s)          (LocalDate/ofInstant (.toInstant ^Date s)
                                  (ZoneId/systemDefault))
    :else                       s))


(defn parse-n [s]
  (if-not (string? s)
    s
    (when-let [^String s (not-empty (str/trim s))]
      (condp re-find s
        #"^\d+$"     (BigDecimal. s)
        #",\d{1,}$"  (BigDecimal.
                       (-> s
                           (str/replace #"[ \.]" "")
                           (str/replace "," ".")))
        #"\.\d{1,}$" (BigDecimal.
                       (str/replace s #"[ ,]" ""))
        (throw (Exception. (str "cannot parse number " s)))))))

;;; Cleaning

(def BANKS
  #{"АТ «ПУМБ»"
    "ПАТ «БАНК ВОСТОК»"
    "АТ \"Райффайзен Банк\""
    "АТ \"АЛЬФА-БАНК\""
    "Акціонерний банк \"Південний\""
    "АТ \"Полтава-банк\""
    "ПрАТ \"БАНК ФАМІЛЬНИЙ\""})

(def TRASH-SENDER-RE
  #"(?x)
    МВПС|
    Транз.счет|
    Транзит за розрах|
    Транз.рах.|
    ІНШІ ГОТІВКОВІ|
    Ощадбанк|
    ОЩАДБАНК|
    ПРАВЕКС")


(defn maybe-drop-sender [s]
  (when-not (or (not s)
                (contains? BANKS s)
                (re-find TRASH-SENDER-RE s))
    s))


(defn capitalize-every [s]
  (->> (str/split s #"\s+")
       (map str/capitalize)
       (str/join " ")))


(defn cleanup-ext [s]
  (-> (reduce (fn [s [re sub]]
                (str/replace s re sub))
        s
        [[#"(?ui)dr\. " ""]
         [#"," ""]
         [#"From (\d+/)?([^ ]+ [^ ]+).*?(?:for (.*))?$"
          (fn [[_m _ full-name message]]
            (str (capitalize-every full-name) " -- " message))]])
      str/trim))


(comment
  (cleanup-ext "From SOME ONE SZEWCZENKI 12/3 79666 LWOW UA UA for Przelew"))


(defn cleanup [s]
  (reduce (fn [s [re sub]]
            (str/replace s re sub))
    s
    [[#"(?ui)(, )?без пдв\.?", ""]
     [#"(?ui)(у т.ч. )?ПДВ \d+% - [\d\,]+ грн.", ""]
     [#"(?ui)Призначення платежу: ", ""]
     ;; with latin letters
     [#"(?ui)(ІПН|IПН|ІПH|IПH) ?\d+", ""]
     [#"(?ui)Рахунок платника [\d\w]+", ""]
     [#"(?ui)платник", ""]
     [#"[A-Z]{2}\d{27}", ""]
     [#"\n", " "]
     [#"  +", " "]
     ;; це коли ім'я два рази повторюється
     [#"(?ui)([^ ]+( [^ ]+)?( [^ ]+)?), \\1", "\\1"]]))


(defn make-comment [sender message]
  (let [sender  (maybe-drop-sender sender)
        message (str/trim (cleanup message))]
    (if sender
      (str sender " -- " message)
      message)))


(defn fmt-amount [amount]
  (if (zero? (mod amount 1))
    (long amount)
    amount))


(def CONFIG
  {:oschad     {:start #(str/includes? % "№ п/п")
                :skip  #(or (= (get % 13) "ТОВ \"ФК \"ЕЛАЄНС\"")
                            ;; no date - no transaction
                            (= (get % 4) nil))
                :fields
                {:id      #(get % 0)
                 :bank    (constantly "Oschad UAH")
                 :date    #(dt (date (get % 4)))
                 :amount  #(parse-n (get % 10))
                 :comment #(make-comment (get % 13) (get % 19))}}
   :privat     {:start #(str/includes? % "Дата проводки")
                :skip  (constantly false)
                :fields
                {:id      #(get % 0)
                 :bank    (constantly "Privat UAH")
                 :date    #(dt (str (get % 1) " " (get % 2)))
                 :amount  #(parse-n (get % 3))
                 :comment #(make-comment (get % 7) (get % 5))}}
   :privat-ext {:start #(str/includes? % "Дата проводки")
                ;; be careful, it has latin i
                :skip  #(or (str/starts-with? (get % 7) "Купiвля")
                            ;; this is when payments return
                            (= "From for" (get % 7)))
                :fields
                {:id      #(get % 0)
                 :bank    #(str "Privat " (get % 4))
                 :date    #(dt (str (get % 1) " " (get % 2)))
                 :amount  #(parse-n (get % 6))
                 :comment #(let [message  (cleanup-ext (get % 7))
                                 amount   (get % 3)
                                 currency (get % 4)]
                             (format "%s (%s %s)"
                               message (fmt-amount amount) currency))}}
   :fondy      {:start #(str/includes? % "Внутрішній ID")
                :skip  #(not= (get % 3) "approved")
                :fields
                {:id      #(get % 0)
                 :bank    #(if (str/starts-with? (get % 6) "SUB-")
                             "Fondy Sub"
                             "Fondy")
                 :date    #(dt (get % 1))
                 :amount  #(parse-n (get % 10))
                 :comment #(let [amount   (get % 4)
                                 currency (get % 5)
                                 card     (get % 12)
                                 country  (get % 13)
                                 _email   (get % 7)]
                             (format "%s ***%s (%s %s)"
                               country card (fmt-amount amount) currency))}}})

;;; Reading

(defn guess-file-enc [f]
  (let [line (.readLine ^java.io.BufferedReader (io/reader f))]
    (if (some
          (fn [^Character c]
            (= Character/OTHER_SYMBOL (Character/getType c)))
          (.toCharArray line))
      "cp1251"
      "utf-8")))

(defn read-csv [f]
  (let [enc  (guess-file-enc f)
        line (with-open [r (io/reader f :encoding enc)]
               (.readLine ^java.io.BufferedReader r))
        sep  (if (< (count (re-seq #"," line))
                    (count (re-seq #";" line)))
               \;
               \,)]
    (csv/read-csv (io/reader f :encoding enc) :separator sep)))


(defn read-xls-cell [^Cell cell]
  (condp = (.getCellType cell)
    CellType/NUMERIC (if (DateUtil/isCellDateFormatted cell)
                       (.getDateCellValue cell)
                       (.getNumericCellValue cell))
    CellType/STRING  (.getStringCellValue cell)
    CellType/BLANK   nil
    (throw (ex-info "Unknown cell type" {:type (.getCellType cell)}))))


(defn read-xls [path]
  (let [s     (io/input-stream path)
        wb    (WorkbookFactory/create s)
        sheet (.getSheetAt wb 0)]
    (for [row sheet]
      (mapv read-xls-cell row))))


(comment
  (def x (read-xls "БФ_Приват_злотий_29_03_2022.xls"))
  (def x (read-xls "/Users/piranha/dev/misc/pzh-finance/clj-finreport/Ощад_28_02_2022.xlsx"))
  (def x (read-xls "/Users/piranha/dev/misc/pzh-finance/clj-finreport/49949__1622114.xlsx"))

  (->> x (drop 5) first))


;;; Convert

(defn parse-row [fields i row]
  (try
    (reduce-kv (fn [acc k getter]
                 (let [value (getter row)]
                   (assoc acc k value)))
      {}
      fields)
    (catch Exception e
      (throw (ex-info (str "Error parsing row: " (str e))
               {:row row :i i} e)))))


(defn detect-bank [data]
  (let [rows (into [] (take 5 data))
        g    (fn [x y] (some-> (get-in rows [x y]) str/trim))]
    (cond
      (= (g 0 0) "Внутрішній ID")                     :fondy
      (= (g 0 0) "Назва Клієнта")                     :oschad
      (= (g 1 6) "Сума еквівалент у гривні")          :privat-ext
      (str/starts-with? (g 0 0) "Виписка по рахунку") :privat
      :else
      (throw (ex-info "Unknown bank!" {:rows rows})))))


(defn process [path content]
  (log/info "Working on" path)
  (let [content (or content (io/file path))
        rows    (cond (re-find #"(?i).csv$" path)    (read-csv content)
                      (re-find #"(?i)\.xlsx?$" path) (read-xls content)
                      :else
                      (throw (ex-info "Unknown file format"
                               {:path path})))

        bank               (detect-bank rows)
        {start-fn :start
         skip-fn  :skip
         fields   :fields} (get CONFIG bank)
        xf                 (comp
                             (remove #(or (empty? (first %))
                                          (< (count %) 5)
                                          (skip-fn %)))
                             (map-indexed (partial parse-row fields))
                             (remove #(or (nil? (:amount %))
                                          (neg? (:amount %)))))]
    (printf "Parsing %s from %s...\n" bank path)
    (->> rows
         (drop-while #(not (start-fn %)))
         rest
         (sequence xf))))

;;; Storing

(defn file-name [path]
  (subs path ((fnil inc -1) (str/last-index-of path "/"))))


(defn replace-ext [path ext]
  (let [idx (str/last-index-of path ".")]
    (str (subs path 0 idx) ext)))


(defn write-csv [path rows]
  (let [fields [:bank :id :date :amount :comment :created_at]
        getter (apply juxt fields)]
    (with-open [w (io/writer (str "clean_"
                               (->
                                 (file-name path)
                                 (replace-ext ".csv"))))]
      (csv/write-csv w [(map name fields)])
      (csv/write-csv w (map getter rows)))))


(defn bank-days [rows]
  (persistent!
    (reduce
      (fn [ret row]
        (conj! ret [(:bank row) (date (:date row))]))
      (transient #{})
      rows)))


(defn write-db [path rows]
  (jdbc/with-transaction [tx db/conn]
    (let [fname    (file-name path)
          delres   (for [[bank d] (bank-days rows)]
                     (db/one tx
                       ["delete from report where bank = ? and date(date) = ?"
                        bank d]))
          fields   [:fname :bank :date :amount :comment]
          mkrow    (fn [row]
                     [fname (:bank row) (:date row) (:amount row) (:comment row)])
          insres   (for [batch (partition-all 1000 rows)]
                     (->> batch
                          (map mkrow)
                          (sql/insert-multi! tx :report fields)
                          count))
          deleted  (apply + (map ::jdbc/update-count delres))
          inserted (apply + insres)]
      {:deleted  deleted
       :inserted inserted})))


(defn process-and-store [mode path content]
  (let [write-fn (case mode
                   :db  write-db
                   :csv write-csv)
        data     (process path content)
        res      (write-fn path data)]
    (log/info "Processing finished" path)
    res))


;;; control

(defn -main [& args]
  (let [{opts true
         args false} (group-by #(str/starts-with? % "-") args)
        opts         (into #{} opts)
        mode     (if (contains? opts "--csv") :csv :db)]
    (doseq [path args]
      (process-and-store mode path nil))))

(comment
  (def x (process "/Users/piranha/dev/misc/pzh-finance/clj-finreport/49949__1622114.xlsx" nil))

  (time (write-db "49949__1622114.xlsx" x))

  (time (process-and-store
          :db
          "/Users/piranha/Downloads/0000002625801021_грн.xls"
          nil)))
