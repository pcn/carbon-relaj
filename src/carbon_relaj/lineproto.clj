(ns carbon-relaj.lineproto
  (:use [clojure.tools.logging :only (debug info warn error)])
  (:require [clojure.core.async :as async]
            [carbon-relaj.util :as util]
            [carbon-relaj.channels :as chan]
            [clojure.string :as s]
            [carbon-relaj.sanitize :as sanitize]
            [taoensso.timbre :as timbre]))


;; Module for handling the carbon line protocol's textual representation

(defn read-carbon-line [line-mapping]
  "line-mapping is a map containing the keys :line and :client-info.
   The line is a whitespace-separated string that looks like this:

     metric-name value timestamp\n

   This is broken up into a vector of [string float float]

   The :socket is provided so that in case of an error, the error can be
   tracked to the remote host and port that provided the bad metric."
  (defn get-address []
    "Extract the address of the client from line-mapping"
    ((line-mapping :client-info) :address))
  (defn get-line []
    "Extract the text of the line from the line-mapping"
    (s/trim (line-mapping :line)))

  (if-not (sanitize/validate-line (get-line))
    (warn "Received a bogus line: " (get-line) " from " (get-address))
    (let [metric-list (s/split (get-line) #"\s+")
          metric-name (get metric-list 0)
          value (get metric-list 1)
          timestamp (get metric-list 2)]
      ;; (println (str "[metric-name value timestamp] is " metric-name " " value " " timestamp))
      (async/go (async/>! chan/spool-channel
                          [metric-name value timestamp])))))
