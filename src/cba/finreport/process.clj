(ns cba.finreport.process
  (:import [java.util Date]
           [java.time LocalDateTime ZonedDateTime LocalDate ZoneId]
           [java.time.temporal ChronoUnit]
           [java.time.format DateTimeFormatter DateTimeParseException]
           [org.apache.poi.ss.usermodel
            WorkbookFactory Row Cell CellType DateUtil])
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


(defn dt
  ([fmt s]
   (LocalDateTime/parse s fmt))
  ([s]
   (cond
     (string? s)             (loop [fmts [dt-fmt fondy2-dt-fmt fondy-dt-fmt]]
                               (when (seq fmts)
                                 (or (try
                                       (LocalDateTime/parse s (first fmts))
                                       (catch DateTimeParseException _))
                                     (recur (rest fmts)))))
     (instance? LocalDate s) (.atStartOfDay ^LocalDate s)
     (instance? Date s)      (LocalDateTime/ofInstant (.toInstant ^Date s)
                               (ZoneId/systemDefault))
     :else                   s)))

(defn date [s]
  (cond
    (string? s)                 (LocalDate/parse s date-fmt)
    (instance? LocalDateTime s) (.toLocalDate ^LocalDateTime s)
    (instance? ZonedDateTime s) (.toLocalDate ^ZonedDateTime s)
    (instance? Date s)          (LocalDate/ofInstant (.toInstant ^Date s)
                                  (ZoneId/systemDefault))
    :else                       s))


(defn kyiv ^ZonedDateTime [^LocalDateTime dt] (.atZone dt (ZoneId/of "Europe/Kiev")))
(defn gmt  ^ZonedDateTime [^LocalDateTime dt] (.atZone dt (ZoneId/of "GMT")))
(defn day-start ^ZonedDateTime [^ZonedDateTime dt] (.truncatedTo dt ChronoUnit/DAYS))
(defn day-end   ^ZonedDateTime [^ZonedDateTime dt] (day-start (.plusDays dt 1)))


(defn parse-n [s]
  (if-not (string? s)
    s
    (when-let [^String s (not-empty (str/trim s))]
      (condp re-find s
        #"^\d+$"     (BigDecimal. s)
        #",\d{1,}$"  (BigDecimal.
                       (-> s
                           ;; Zs is a "Space Separator" Unicode class
                           (str/replace #"[\p{Zs}\.\\]" "")
                           (str/replace "," ".")))
        #"\.\d{1,}$" (BigDecimal.
                       (str/replace s #"[\p{Zs},\\]" ""))
        (throw (Exception. (str "cannot parse number " s)))))))


(comment
  (parse-n "325 041,63"))


(def TAG-RE #"(?u)#(\p{L}[\p{L}\d_\-\.]*[\p{L}\d])")


(defn parse-card-oid
  "Parses order id from card system to get various props.

  Order id should be of format:

    [SUB-]...~#tag1~#tag2~_hidden1~_hidden2"
  [oid]
  (let [tags    (re-seq TAG-RE oid)
        hiddens (re-seq #"_([^~]+)" oid)]
    {:sub     (str/starts-with? oid "SUB-")
     :tags    (not-empty (mapv second tags))
     :hiddens (not-empty (mapv second hiddens))}))


(defn parse-tags [msg]
  ;; tag starts with a letter, ends with a letter or number
  (let [tags (re-seq TAG-RE msg)]
    (some->> (not-empty (mapv second tags))
      (into-array String))))

;;; Cleaning

(def OWN-ACCOUNTS #{"UA223226690000026007300905964"})


(def TRASH-SENDER-RE
  #"(?x)
    МВПС
|Транз.счет
|Транзит за розрах
|Транз.рах.
|ІНШІ ГОТІВКОВІ
|Ощадбанк
|ОЩАДБАНК
|ПРАВЕКС
|ПУМБ
|АКОРДБАНК
|NEOBANK
|\bБанк\b
|\bБАНК\b
|\bбанк\b")


(defn maybe-drop-sender [s]
  (when-not (or (not s)
                (re-find TRASH-SENDER-RE s))
    s))


(defn short-name [s]
  (let [i (str/index-of s " ")]
    (format "%s %s."
      (str/capitalize (subs s 0 i))
      (str/capitalize (subs s (+ i 1) (+ i 2))))))


(defn capitalize-every [s]
  (str/replace s #"\b[^ ]+\b" (fn [m] (str/capitalize m))))


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


(def NAME-RE
  #"(?u)(\p{Lu}[\p{Lu}\p{Ll}\-]+ \p{Lu}[\p{Lu}\p{Ll}\-]+)( \p{Lu}[\p{Lu}\p{Ll}\-]+)")


(def CLEANUP-RES
  [[#"(?ui)(, )?без пдв\.?", ""]
   [#"(?ui)(у т.ч. )?ПДВ \d+% - [\d\,]+ грн.", ""]
   [#"(?ui)Призначення платежу: ", ""]
   ;; with latin letters
   [#"(?ui)(ІПН|IПН|ІПH|IПH|ИНН) ?\d+", ""]
   [#"(?ui)квитанцiя [\d\.\-]+" ""]
   [#"(?ui)Рахунок платника [\d\w]+", ""]
   ;; oschad sometimes strips data in the end
   [#"(?ui)Рахунок платника$", ""]
   [#"(?ui)Рахунок$", ""]
   [#"(?ui)квитанцiя$" ""]
   [#"(?ui)\b(вул|бул|ул|пров|пр)\..*$" ""]
   [#"(?ui)кв.? ?\d+" ""]
   [#"(?ui)\bм\..*$" ""]
   ;; removing names
   ;; when there is a name: uppercase, followed by non-uppercases and a space, two or three times
   [NAME-RE (fn [m]
              (capitalize-every (second m)))]
   [#"(?u)([\p{L}\-]+ [\p{L}\-]+), \1" "$1"]
   [#"[A-Z]{2}\d{27}", ""]
   [#"\d{7,}", ""]                      ; phones, ids, etc
   [#"\n", " "]
   [#"  +", " "]
   ;; leftover punctuation
   [#"\s+$" ""]
   [#". \)" ".)"]
   [#". ," ".,"]
   [#"[\(\)\-\.\,\s\d]{4,}" ""]
   [#",$" ""]])


(defn cleanup [s]
  (reduce (fn [s [re sub]]
            (str/replace s re sub))
    s
    CLEANUP-RES))


(defn make-comment [sender message]
  (let [message (cond-> (cleanup message)
                  (not (str/blank? sender)) (str/replace sender "")
                  true                      (str/trim))
        sender  (if (re-matches NAME-RE sender)
                  (str/replace sender NAME-RE
                    (fn [m] (capitalize-every (second m))))
                  (maybe-drop-sender sender))]
    (if sender
      (str sender " -- " message)
      message)))


(defn fmt-amount [amount]
  (if (zero? (mod amount 1))
    (long amount)
    amount))


(def ^:dynamic *currency* "oschad-ext currency" nil)


(def CONFIG
  ;; NOTE не забувай юзази some-> тому що трапляються рядки без даних
  {:oschad     {:start #(str/includes? % "№ п/п")
                :skip  #(or (#{"ТОВ \"АВТО УА ГРУП\""
                               "ТОВ \"ФК \"ЕЛАЄНС\""
                               "Іщенко Максим Федорович"}
                              (get % 13))
                            (some-> (get % 13) (str/starts-with? "Плат.интер-эквайрин через LiqPay"))
                            (some-> (get % 19) (str/starts-with? "Повернення "))
                            (some->> (get % 19) (re-find #"(?i)нев[іi]рний код отримувача"))
                            ;; no date - no transaction
                            (= (get % 4) nil))
                :fields
                {:id      #(get % 2)
                 :bank    (constantly "Oschad UAH")
                 :date    #(kyiv (dt (date (get % 4))))
                 :amount  #(parse-n (get % 10))
                 :comment #(make-comment (get % 13) (get % 19))
                 :tags    #(parse-tags (get % 19))}}
   :oschad-ext {:start    #(str/includes? % "№ п/п")
                :skip     #(or (nil? (get % 1))
                               (some-> (get % 24) (str/starts-with? "Повернення ")))
                :currency (fn [lazy-rows]
                            (->> (get (nth lazy-rows 5) 4)
                                 (re-find #"\((\w+)\)")
                                 second))
                :fields
                {:id      #(get % 1)
                 :bank    (fn [_] (str "Oschad " *currency*))
                 :date    #(kyiv (dt (date (get % 3))))
                 :amount  #(parse-n (get % 14))
                 :comment #(let [msg    (cleanup-ext (get % 24))
                                 amount (get % 12)]
                             (format "%s (%s %s)"
                               msg (fmt-amount amount) *currency*))
                 :tags    #(parse-tags (get % 24))}}
   :privat     {:start #(str/includes? % "Дата проводки")
                :skip  #(or (when-let [msg (get % 5)]
                              (or (str/starts-with? msg "Повернення ")
                                  (str/starts-with? msg "Конвертац")))
                            (contains? OWN-ACCOUNTS (get % 8)))
                :fields
                {:id      #(get % 0)
                 :bank    (constantly "Privat UAH")
                 :date    #(kyiv (dt (str (get % 1) " " (get % 2))))
                 :amount  #(parse-n (get % 3))
                 :comment #(make-comment (get % 7) (get % 5))
                 :tags    #(parse-tags (get % 5))}}
   :privat-ext {:start #(str/includes? % "Дата проводки")
                ;; be careful, it has latin i
                :skip  #(when-let [msg (get % 7)]
                          (or (= msg "From for") ; this is when payments return
                              (str/starts-with? msg "Повернення ")
                              (str/starts-with? msg "Купiвля")
                              (str/starts-with? msg "Конвертац")
                              (str/includes? msg "технiчне проведення усунення розбалансу")))
                :fields
                {:id      #(get % 0)
                 :bank    #(str "Privat " (get % 4))
                 :date    #(kyiv (dt (str (get % 1) " " (get % 2))))
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
                 :date    #(kyiv (dt (get % 1)))
                 :amount  #(parse-n (get % 10))
                 :comment #(let [amount   (get % 4)
                                 currency (get % 5)
                                 card     (get % 12)
                                 country  (get % 13)
                                 _email   (get % 7)]
                             (format "%s ***%s (%s %s)"
                               country card (fmt-amount amount) currency))
                 :tags    #(:tags (parse-card-oid (get % 6)))}}
   :manual     {:start #(str/includes? % "Тип надходження")
                :skip  (constantly false)
                :fields
                {:id      (constantly "")
                 :bank    #(if (= "готівка" (get % 3))
                             "Cash"
                             (get % 3))
                 :date    #(kyiv (dt (get % 0)))
                 :amount  #(parse-n (get % 1))
                 :comment #(get % 2)
                 :tags    #(parse-tags (get % 2))}}

   :spending {:start #(str/includes? % "Постачальник")
              :skip  (constantly false)
              :fields
              {:id     (constantly "")
               :bank   (constantly "Spending")
               :date   #(kyiv (dt (date (get % 10))))
               :amount #(parse-n (get % 16))
               :name   #(let [cnt  (get % 14)
                              unit (get % 13)]
                          (if (and cnt (pos? cnt))
                            (format "%s (%s %s)" (get % 12) (long cnt) unit)
                            (get % 12)))
               :target #(get % 11)}}})


(defn detect-bank [data]
  (let [rows (into [] (take 15 data))
        g    (fn [x y] (some-> (get-in rows [x y]) str str/trim))]
    (cond
      (= (g 0 0) "Внутрішній ID")                              :fondy
      (= (g 10 14) "КТ рахунку (еквівалент)")                  :oschad-ext
      (= (g 0 0) "Назва Клієнта")                              :oschad
      (= (g 1 6) "Сума еквівалент у гривні")                   :privat-ext
      (some-> (g 0 0) (str/starts-with? "Виписка по рахунку")) :privat
      (= (g 0 3) "Тип надходження")                            :manual
      (= (g 0 0) "Постачальник")                               :spending
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
  (let [enc (guess-file-enc f)]
    (with-open [rdr (io/reader f :encoding enc)]
      (.mark rdr 100)
      (let [line (.readLine ^java.io.BufferedReader rdr)
            sep  (if (< (count (re-seq #"," line))
                        (count (re-seq #";" line)))
                   \;
                   \,)]
        (.reset rdr)
        (into []
          (csv/read-csv rdr :separator sep))))))


(defn read-xls-cell [^Cell cell]
  (condp = (.getCellType cell)
    CellType/NUMERIC (if (DateUtil/isCellDateFormatted cell)
                       (.getDateCellValue cell)
                       (.getNumericCellValue cell))
    CellType/STRING  (.getStringCellValue cell)
    CellType/BLANK   nil
    CellType/FORMULA (.getNumericCellValue cell)
    (throw (ex-info "Unknown cell type" {:type (.getCellType cell)}))))


(defn read-xls [path]
  (let [s  (io/input-stream path)
        wb (WorkbookFactory/create s)]
    (for [sheet    (iterator-seq (.sheetIterator wb))
          ^Row row sheet]
      (mapv #(when-let [cell (.getCell row %)]
               (read-xls-cell cell))
        (range (.getLastCellNum row))))))


(comment
  (def x (read-xls "БФ_Приват_злотий_29_03_2022.xls"))
  (def x (read-xls "/Users/piranha/dev/misc/pzh-finance/clj-finreport/Ощад_28_02_2022.xlsx"))
  (def x (read-xls "/Users/piranha/Downloads/АКТИ+Рахунки _Леся_2022.xlsx"))
  (def wb (-> "/Users/piranha/Downloads/АКТИ+Рахунки _Леся_2022.xlsx"
              io/input-stream
              WorkbookFactory/create))
  (def s (.getSheetAt wb 0))
  (def r (.getRow s 2))
  (.getFirstCellNum r)
  (.getLastCellNum r)

  (prn (map identity (.getRow s 0)))
  (prn (map #(.getColumnIndex %) (.getRow s 2)))

  (def w (take 2 x))
  (parse-row (-> CONFIG :spending :fields) "UAH" 1 (first (drop 2 x)))

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
               (let [skip? (cond
                             (every? nothing? row) :empty-row
                             (< (count row) 3)     :too-short
                             (skip-fn row)         :skipped
                             :else                 nil)
                     res   (when-not skip?
                             (parse-row fields currency i row))
                     skip? (cond
                             skip?                skip?
                             (nil? (:amount res)) :no-amount
                             (neg? (:amount res)) :neg-amount
                             :else                nil)]
                 (if skip?
                   (do
                     (swap! *skipped conj [skip? row])
                     nil)
                   res)))
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
  (reduce
    (fn [m row]
      (update m [(:bank row) (day-start (:date row))] (fnil inc 0)))
    {}
    rows))


(def DELETE-Q
  "delete from %s
    where bank = ?
      and date >= ?::timestamp
      and date < ?::timestamp")


(def FIELDS
  {:donate   [:fname :bank :date :amount :comment :tags :hiddens]
   :spending [:fname :bank :date :amount :name :target]})


(defn write-db [path rows]
  (jdbc/with-transaction [tx db/conn]
    (let [fname     (file-name path)
          spending? (= "Spending" (:bank (first rows)))
          table     (if spending? :spending :report)
          fields    (if spending?
                      (:spending FIELDS)
                      (:donate FIELDS))
          mkrow     (if spending?
                      (fn [row]
                        [fname (:bank row) (:date row) (:amount row)
                         (:name row) (:target row)])
                      (fn [row]
                        [fname (:bank row) (:date row) (:amount row)
                         (:comment row)
                         (db/string-array (:tags row))
                         (db/string-array (:hiddens row))]))

          days     (bank-days rows)
          delete-q (format DELETE-Q (name table))
          delres   (for [[bank d] (keys days)]
                     (db/one tx [delete-q bank (day-start d) (day-end d)]))
          insres   (for [batch (partition-all 1000 rows)]
                     (count
                       (sql/insert-multi! tx table fields (map mkrow batch))))
          deleted  (apply + (map ::jdbc/update-count delres))
          inserted (apply + insres)]
      {:deleted  deleted
       :inserted inserted
       :days     days})))


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
        mode         (if (contains? opts "--csv") :csv :db)]
    (doseq [path args]
      (process-and-store mode path nil))))

(comment
  (def x (read-xls (io/file "/Users/piranha/Downloads/БФ_Приват_грн_01_липня.xls")))
  (def x (process "/Users/piranha/Downloads/БФ_Приват_грн_01_липня.xls" nil))

  (time (write-db "49949__1622114.xlsx" x))

  (time (process-and-store
          :db
          "/Users/piranha/Downloads/0000002625801021_грн.xls"
          nil)))
