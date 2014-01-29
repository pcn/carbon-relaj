(ns carbon-relaj.config
  (:gen-class)
  (:use [clojure-ini.core]))

(defn read-config [config-file-path]
  "Read and parse the config file.
   TODO: check file existence, etc"
  (read-ini config-file-path :keywordize true :comment-char "#"))
