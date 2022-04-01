(ns build
  (:require [clojure.tools.build.api :as b]
            [clojure.java.io :as io]))


(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))
(def version (b/git-process {:git-args "describe --tags"}))
(def uber-file (format "target/finreport-%s.jar" version))


(defn clean [_]
  (b/delete {:path "target"}))


(defn uber [_]
  (clean nil)
  (b/copy-dir {:src-dirs   ["src" "resources"]
               :target-dir class-dir})
  (spit (io/file class-dir "VERSION") version)
  (b/compile-clj {:basis     basis
                  :src-dirs  ["src"]
                  :class-dir class-dir})
  (b/uber {:class-dir class-dir
           :uber-file uber-file
           :basis     basis
           :main      'uapatron.main}))
