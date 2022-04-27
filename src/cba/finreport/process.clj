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
(def fondy2-dt-fmt (DateTimeFormatter/ofPattern "yyyy-MM-dd HH:mm:ss"))
(def date-fmt (DateTimeFormatter/ofPattern "dd.MM.yyyy"))

(defn dt [s]
  (cond
    (string? s)             (loop [fmts [dt-fmt
                                         fondy2-dt-fmt
                                         fondy-dt-fmt]]
                              (when (seq fmts)
                                (or (try
                                      (LocalDateTime/parse s (first fmts))
                                      (catch DateTimeParseException _))
                                    (recur (rest fmts)))))
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


(defn parse-card-oid
  "Parses order id from card system to get various props.

  Order id should be of format:

    [SUB-]...-#tag1-#tag2-!hidden1-!hidden2"
  [oid]
  (let [tags    (re-seq #"#([^-]+)" oid)
        hiddens (re-seq #"!([^-]+)" oid)]
    {:sub     (str/starts-with? oid "SUB-")
     :tags    (some->> (not-empty (mapv second tags))
                (into-array String))
     :hiddens (some->> (not-empty (mapv second hiddens))
                (into-array String))}))


(defn parse-tags [msg]
  (let [tags (re-seq #"#([^ ]+)" msg)]
    (some->> (not-empty (mapv second tags))
      (into-array String))))


;;; Cleaning

(def OWN-ACCOUNTS #{"UA223226690000026007300905964"})


(def TRASH-SENDER
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
                (contains? TRASH-SENDER s)
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


(def ^:dynamic *currency* "oschad-ext currency" nil)


(def CONFIG
  {:oschad     {:start #(str/includes? % "№ п/п")
                :skip  #(or (#{"ТОВ \"АВТО УА ГРУП\""
                               "ТОВ \"ФК \"ЕЛАЄНС\""
                               "Іщенко Максим Федорович"}
                              (get % 13))
                            (some-> (get % 19) (str/starts-with? "Повернення коштів"))
                            (some-> (get % 19) (str/starts-with? "Кред.заб. за прийн.плат."))
                            ;; no date - no transaction
                            (= (get % 4) nil))
                :fields
                {:id      #(get % 0)
                 :bank    (constantly "Oschad UAH")
                 :date    #(dt (date (get % 4)))
                 :amount  #(parse-n (get % 10))
                 :comment #(make-comment (get % 13) (get % 19))
                 :tags    #(parse-tags (get % 19))}}
   :oschad-ext {:start    #(str/includes? % "№ п/п")
                :skip     #(or (nil? (get % 1))
                               (some-> (get % 24) (str/starts-with? "Повернення коштів")))
                :currency (fn [lazy-rows]
                            (->> (get (nth lazy-rows 5) 4)
                                 (re-find #"\((\w+)\)")
                                 second))
                :fields
                {:id      #(get % 1)
                 :bank    (fn [_] (str "Oschad " *currency*))
                 :date    #(dt (date (get % 3)))
                 :amount  #(parse-n (get % 14))
                 :comment #(let [msg    (cleanup-ext (get % 24))
                                 amount (get % 12)]
                             (format "%s (%s %s)"
                               msg (fmt-amount amount) *currency*))
                 :tags    #(parse-tags (get % 24))}}
   :privat     {:start #(str/includes? % "Дата проводки")
                :skip  #(or (some-> (get % 7) (str/starts-with? "Повернення "))
                            (some-> (get % 7) (str/starts-with? "Кредиторська заборгованість"))
                            (contains? OWN-ACCOUNTS (get % 8)))
                :fields
                {:id      #(get % 0)
                 :bank    (constantly "Privat UAH")
                 :date    #(dt (str (get % 1) " " (get % 2)))
                 :amount  #(parse-n (get % 3))
                 :comment #(make-comment (get % 7) (get % 5))
                 :tags    #(parse-tags (get % 5))}}
   :privat-ext {:start #(str/includes? % "Дата проводки")
                ;; be careful, it has latin i
                :skip  #(let [msg (get % 7)]
                          (or (= msg "From for") ; this is when payments return
                              (some-> msg (str/starts-with? "Купiвля"))
                              (some-> msg (str/includes? "технiчне проведення усунення розбалансу"))))
                :fields
                {:id      #(get % 0)
                 :bank    #(str "Privat " (get % 4))
                 :date    #(dt (str (get % 1) " " (get % 2)))
                 :amount  #(parse-n (get % 6))
                 :comment #(let [message  (cleanup-ext (get % 7))
                                 amount   (get % 3)
                                 currency (get % 4)]
                             (format "%s (%s %s)"
                               message (fmt-amount amount) currency))
                 :tags    #(parse-tags (get % 7))}}
   :fondy      {:start #(str/includes? % "Внутрішній ID")
                :skip  #(not= (get % 3) "approved")
                :fields
                {:id      #(get % 6)
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
                               country card (fmt-amount amount) currency))
                 :tags    #(:tags (parse-card-oid (get % 6)))}}
   :cash       {:start #(str/includes? % "Тип надходження")
                :skip  (constantly false)
                :fields
                {:id      (constantly "")
                 :bank    (constantly "Cash")
                 :date    #(dt (get % 0))
                 :amount  #(parse-n (get % 1))
                 :comment #(get % 2)
                 :tags    #(parse-tags (get % 2))}}})


(defn detect-bank [data]
  (let [rows (into [] (take 15 data))
        g    (fn [x y] (some-> (get-in rows [x y]) str str/trim))]
    (cond
      (= (g 0 0) "Внутрішній ID")                     :fondy
      (= (g 10 14) "КТ рахунку (еквівалент)")         :oschad-ext
      (= (g 0 0) "Назва Клієнта")                     :oschad
      (= (g 1 6) "Сума еквівалент у гривні")          :privat-ext
      (str/starts-with? (g 0 0) "Виписка по рахунку") :privat
      (and (= (g 0 2) "Тип надходження")
           (= (g 1 2) "готівка"))                     :cash
      :else
      (throw (ex-info "Unknown bank!" {:rows rows})))))


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
    CellType/FORMULA nil
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

(defn parse-row [fields currency i row]
  (binding [*currency* currency]
    (try
      (reduce-kv (fn [acc k getter]
                   (let [value (getter row)]
                     (assoc acc k value)))
        {}
        fields)
      (catch Exception e
        (throw (ex-info (str "Error parsing row: " (str e))
                 {:row row :i i}
                 e))))))


(defn nothing? [v]
  (if (seq? v)
    (empty? v)
    (nil? v)))


(defn reporting-remove [])


(defn process [path content]
  (log/info "Working on" path)
  (let [content (or content (io/file path))
        rows    (cond (re-find #"(?i).csv$" path)    (read-csv content)
                      (re-find #"(?i)\.xlsx?$" path) (read-xls content)
                      :else
                      (throw (ex-info "Unknown file format"
                               {:path path})))
        bank    (detect-bank rows)

        {start-fn    :start
         skip-fn     :skip
         currency-fn :currency
         fields      :fields} (get CONFIG bank)
        currency              (when currency-fn
                                (currency-fn rows))
        *skipped              (atom [])

        func (fn [i row]
               (let [skip? (or (nothing? (first row))
                               (< (count row) 3)
                               (skip-fn row))
                     res   (when-not skip?
                             (parse-row fields currency i row))]
                 (if-not (or (nil? (:amount res))
                             (neg? (:amount res)))
                   res
                   (do
                     (swap! *skipped conj row)
                     nil))))
        xf   (comp
               (map-indexed func)
               (remove nil?))]
    (printf "Parsing %s from %s...\n" bank path)
    {:bank    bank
     :data    (->> rows
                   (drop-while #(not (start-fn %)))
                   rest
                   (into [] xf))
     :skipped @*skipped}))

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


(def DELETE-Q
  "delete from report
    where bank = ?
      and date >= ?::date
      and date < (?::date + '1 day'::interval)")


(defn write-db [path rows]
  (jdbc/with-transaction [tx db/conn]
    (let [fname    (file-name path)
          delres   (for [[bank d] (bank-days rows)]
                     (db/one tx [DELETE-Q bank d d]))
          fields   [:fname :bank :date :amount :comment :tags :hiddens]
          mkrow    (fn [row]
                     [fname (:bank row) (:date row) (:amount row) (:comment row)
                      (:tags row) (:hiddens row)])
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
        res      (write-fn path (:data data))]
    (log/info "Processing finished" path)
    (merge res (dissoc data :data))))


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
