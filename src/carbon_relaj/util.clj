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
