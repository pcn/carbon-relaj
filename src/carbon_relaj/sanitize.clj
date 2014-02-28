(ns carbon-relaj.sanitize
  (:require ;; [taoensso.timbre :as timbre]
            [bouncer [core :as b] [validators :as v]]
            [clojure.string :as s] ))

;; Provides useful Timbre aliases in this ns - maybe move to conf since
;; it may e.g. get log-level changed at runtime?
;; (timbre/refer-timbre)


(defn line-has-content [text-of-the-line]
  (not (empty? text-of-the-line)))

(defn line-has-three-fields [text-of-the-line]
  (= 3 (count (s/split text-of-the-line #"\s+"))))

(defn try-to-float [string-to-convert-to-a-number]
   (try (Float/parseFloat string-to-convert-to-a-number) (catch Exception e "")))


(defn validate-line-has-content [text-of-the-line]
  "Trim whitespace from the beginning and end of the line and check it out"
  (b/valid? {:text text-of-the-line}
            {:text [line-has-content]}))

(defn validate-line-is-the-right-size [text-of-the-line]
  (b/valid? {:text text-of-the-line}
           {:text [line-has-three-fields]}))


(defn validate-line-has-a-string-and-numbers [text-of-the-line]
  (let [list-of-the-line (s/split text-of-the-line #"\s+")]
    ;; XXX: log the failure
    (b/valid? {:metric-name (get list-of-the-line 0)
               :metric-value (try-to-float (get list-of-the-line 1))
               :metric-timestamp (try-to-float (get list-of-the-line 2))}
              {:metric-name [[v/required :message "The metric name isn't present"]]
               :metric-value [[v/required :message "The metric value isn't present"]
                              [v/number :message "The metric isn't a number"]]
               :metric-timestamp [[v/required :message "The timestamp isn't present"]
                                  [v/number :message "The timestamp isn't a number"]
                                  [v/positive :message "The timestamp isn't positive"]]})))

(defn validate-line [text-of-the-line]
  "Checks that a line's content is OK"
  (b/valid? {:text text-of-the-line}
            {:text [validate-line-has-content
                    validate-line-is-the-right-size
                    validate-line-has-a-string-and-numbers]}))

;; (defn validate-line [text-of-the-line]
;;   true)
