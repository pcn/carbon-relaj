(ns carbon-relaj.sanitize
  (:require [taoensso.timbre :as timbre]
            [bouncer [core :as b] [validators :as v]]
            [clojure.string :as s] ))

;; Provides useful Timbre aliases in this ns - maybe move to conf since
;; it may e.g. get log-level changed at runtime?
(timbre/refer-timbre)


(defn line-has-content [text-of-the-line]
  (not (empty? text-of-the-line)))

(defn line-has-three-fields [text-of-the-line]
  (= 3 (count (s/split text-of-the-line #"\s+"))))

(defn validate-line-has-content [text-of-the-line]
  "Trim whitespace from the beginning and end of the line and check it out"
  (b/valid? {:text text-of-the-line}
            {:text [line-has-content]}))

(defn validate-line-is-the-right-size [text-of-the-line]
  (b/valid {:text text-of-the-line}
           {:text [line-has-three-fields]}))

(defn validate-line-has-a-string-and-numbers [text-of-the-line]
  (let list-of-the-line (s/split text-of-the-line #"\s+")
       (b/valid {:metric-name (get list-of-the-line 0)
                 :metric-value (get list-of-the-line 1)
                 :metric-timestamp (get list-of-the-line 2)}
                {:metric-name [v/required]
                 :metric-value [v/required v/number]
                 :metric-timestamp [v/required v/number v/positive]})))


(defn validate-content [text-of-the-line]
  (b/valid {:text text-of-the-line}
           {:text [validate-line-has-content
                   validate-line-is-the-right-size
                   validate-line-has-a-string-and-numbers]}))
