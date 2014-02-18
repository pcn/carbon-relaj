(ns carbon-relaj.util
  (:gen-class)
  (:use [clojure.main :only (repl)]
        [clojure.tools.logging :only (debug info warn error)]))

(defn exit-error
  "exits with a message, maybe an exit code"
  ([]
     (exit-error "An error was encountered, and the system will now exit.  Sorry it didn't work out." 1))
  ([message]
     (exit-error  message 1))
  ([message errorcode]
     (error message)
     (System/exit (int errorcode))))


;; Check that we're using a jvm that gives fs the ability to do
;; hard links.
(defn check-jvm-version []
  (if (< 1 (count (filter true? (clojure.core/map #(= "link" (str (first %)))
                                                  (seq (ns-publics (the-ns 'me.raynes.fs)))))))
    (exit-error
     (str "This jre doesn't provide you with the ability to do hard links.  Use java versions >=1.7.\n"
          "Exiting with a sad face.  :(\n")
     100)))
