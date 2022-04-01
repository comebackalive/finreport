(ns cba.finreport.process
  (:import [java.time LocalDateTime LocalDate]
           [java.time.format DateTimeFormatter]
           [org.apache.poi.ss.usermodel WorkbookFactory Cell CellType
            DateUtil])
  (:require [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.data.csv :as csv]
            [next.jdbc.sql :as sql]

            [cba.finreport.db :as db]))


(set! *warn-on-reflection* true)

;;; Parsing

(def dt-fmt (DateTimeFormatter/ofPattern "dd.MM.yyyy HH:mm:ss"))
(def date-fmt (DateTimeFormatter/ofPattern "dd.MM.yyyy"))

(defn parse-dt [s]
  (if (string? s)
    (LocalDateTime/parse s dt-fmt)
    s))

(defn parse-d [s]
  (if (string? s)
    (.atStartOfDay (LocalDate/parse s date-fmt))
    s))


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

(defn maybe-drop-sender [s]
  (when-not (or (not s)
                (str/includes? s "МВПС")
                (str/includes? s "Транз.счет")
                (str/includes? s "Транзит за розрах")
                (str/includes? s "Транз.рах."))
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
         [#"From (\d+/)?([^ ]+ [^ ]+).*?(for .*)?$"
          (fn [[_m _ full-name message]]
            (str (capitalize-every full-name) " " message))]])
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


(def CONFIG
  {:oschad     {:start #(str/includes? % "№ п/п")
                :skip  #(or (= (get % 13) "ТОВ ФК \"ЕЛАЄНС\"")
                            ;; no date - no transaction
                            (= (get % 4) nil))
                :fields
                {:id      #(get % 0)
                 :bank    (constantly "Oschad UAH")
                 :date    #(parse-d (get % 4))
                 :amount  #(parse-n (get % 10))
                 :comment #(make-comment (get % 13) (get % 19))}}
   :privat     {:start #(str/includes? % "Дата проводки")
                :skip  (constantly false)
                :fields
                {:id      #(get % 0)
                 :bank    (constantly "Privat UAH")
                 :date    #(parse-dt (str (get % 1) " " (get % 2)))
                 :amount  #(parse-n (get % 3))
                 :comment #(make-comment (get % 7) (get % 5))}}
   :privat-ext {:start #(str/includes? % "Дата проводки")
                :skip  (constantly false)
                :fields
                {:id      #(get % 0)
                 :bank    #(str "Privat " (get % 4))
                 :date    #(parse-dt (str (get % 1) " " (get % 2)))
                 :amount  #(parse-n (get % 6))
                 :comment #(let [message  (cleanup-ext (get % 7))
                                 amount   (get % 3)
                                 currency (get % 4)]
                             (format "%s (%s %s)"
                               message amount currency))}}
   :fondy      {:start #(str/includes? % "Внутрішній ID")
                :skip  #(not= (get % 3) "approved")
                :fields
                {:id      #(get % 0)
                 :bank    #(if (str/starts-with? (get % 6) "SUB-")
                             "Fondy Sub"
                             "Fondy")
                 :date    #(parse-dt (get % 1))
                 :amount  #(get % 10)
                 :comment #(let [amount   (get % 4)
                                 currency (get % 5)
                                 card     (get % 12)
                                 country  (get % 13)
                                 _email   (get % 7)]
                             (format "%s ***%s (%s %s)"
                               country card amount currency))}}})

;;; Reading

(defn read-csv [path]
  (csv/read-csv (io/reader path)))


(defn read-cell [^Cell cell]
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
    #_ (for [i (range (.getLastRowNum sheet))]
         (mapv str (.getRow sheet i)))
    (for [row sheet]
      (mapv read-cell row))))

(comment
  (def x (read-xls "БФ_Приват_злотий_29_03_2022.xls"))
  (def x (read-xls "/Users/piranha/dev/misc/pzh-finance/clj-finreport/Ощад_28_02_2022.xlsx"))

  (->> x (drop 20) first prn))


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


(defn detect [data]
  (let [rows (into [] (take 5 data))]
    (cond
      (= (get-in rows [0 0]) "Внутрішній ID")                     :fondy
      (= (get-in rows [0 0]) "Назва Клієнта")                     :oschad
      (= (get-in rows [1 6]) "Сума еквівалент у гривні")          :privat-ext
      (str/starts-with? (get-in rows [0 0]) "Виписка по рахунку") :privat
      :else
      (throw (ex-info "Unknown bank!" {:rows rows})))))


(defn process [path content]
  (println "Working on" path)
  (let [content (or content (io/file path))
        rows    (cond (str/ends-with? path ".csv") (read-csv content)
                      (re-find #"\.xlsx?$" path)   (read-xls content)
                      :else
                      (throw (ex-info "Unknown file format"
                               {:path path})))

        bank               (detect rows)
        {start-fn :start
         skip-fn  :skip
         fields   :fields} (get CONFIG bank)
        xf                 (comp (remove #(or (empty? (first %))
                                              (< (count %) 5)
                                              (skip-fn %)))
                             (map-indexed (partial parse-row fields))
                             (remove #(nil? (:amount %))))]
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


(defn write-db [path rows]
  (let [fname  (file-name path)
        fields [:fname :bank :date :amount :comment]]
    (db/q ["delete from report report where fname = ?" fname])
    (apply +
      (for [batch (partition-all 1000 rows)]
        (->> batch
             (map (fn [row]
                    [fname (:bank row) (:date row) (:amount row) (:comment row)]))
             (sql/insert-multi! db/conn :report fields)
             count)))))


;;; control

(defn -main [& args]
  (let [{opts true
         args false} (group-by #(str/starts-with? % "-") args)
        opts         (into #{} opts)
        write-fn     (if (contains? opts "--csv")
                       write-csv
                       write-db)]
    (doseq [path args]
      (->> (process path nil)
           (write-fn path)))))

(comment
  (time (-main "--csv" "/Users/piranha/dev/misc/pzh-finance/clj-finreport/Ощад_28_02_2022.xlsx")))
