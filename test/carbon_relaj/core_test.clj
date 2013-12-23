(ns carbon-relaj.core-test
  (:require [clojure.test :refer :all]
            [carbon-relaj.core :refer :all]))

(deftest test-make-file-name
  (testing "Make-file-name"
    (let [dir-name "/tmp/foo/temp"
          thread-id (.getId (Thread/currentThread))
          the-time (carbon-relaj.core/make-time-map)
          time-float-str (str (the-time :seconds) "." (format "%03d" (the-time :ms))) ]
      (is (= (carbon-relaj.core/make-file-name dir-name thread-id the-time)
             (str dir-name "/"  thread-id "-" time-float-str))))))

; (deftest a-test
;   (testing "FIXME, I fail."
;     (is (= 0 1))))
