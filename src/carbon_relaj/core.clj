(ns carbon-relaj.core
  (:gen-class :main true)
  (:import (java.net InetAddress ServerSocket Socket SocketException)
           (java.io InputStreamReader OutputStream
                    OutputStreamWriter PrintWriter
                    InputStreamReader BufferedReader)
           (clojure.lang LineNumberingPushbackReader))
  (:use [clojure.main :only (repl)])
  (:require [clojure.core.async :as async]
            [carbon-relaj.files :as files]
            [carbon-relaj.util :as util]
            [carbon-relaj.config :as cf]
            [carbon-relaj.sanitize :as sanitize]
            [lamina.core :as lamina]
            [aleph.tcp :as aleph]
            [gloss.core :as gloss]
            [taoensso.timbre :as timbre]
            [clojure.string :as s]))



;; Initialization and sanity checks

;; First check that we're using a jvm that gives fs the ability to do
;; hard links.
(defn check-jvm-version []
  (if (< 1 (count (filter true? (clojure.core/map #(= "link" (str (first %)))
                                                  (seq (ns-publics (the-ns 'me.raynes.fs)))))))
    (util/exit-error
     (str "This jre doesn't provide you with the ability to do hard links.  Use java versions >=1.7.\n"
          "Exiting with a sad face.  :(\n")
     100)))

(check-jvm-version)

;; Provides useful Timbre aliases in this ns - maybe move to conf since
;; it may e.g. get log-level changed at runtime?
(timbre/refer-timbre)

;; Read configuration and command line
(cf/read-config)

;; Channels
(def carbon-channel (async/chan (cf/*config* :channel-queue-size))) ; Channel for input of carbon line proto from the network.
(def spool-channel (async/chan (cf/*config* :channel-queue-size))) ; Channel for metrics to head to disk.


;; Test with
;; (-main) (async/>!! spool-channel ["a.b.c.d" ((files/make-time-map) :float)  ((files/make-time-map) :float)])
(defn write-metric-to-file
  "Pulls a metric off of the channel spool-channel.

   Each metric is a vector of [metric value timestamp]
   It will be written out as a 1-line json document in a file with
   many such lines.  The 1-line document is a defensive measure since
   the server has to accept a lot of possibly strange things on the wire.
   Using json will encode it in a readable manner (in most cases).

   This needs to be run on an independent thread that will read from an
   async channel, and write to disk, rotating files as needed.  This
   can't be called from a go block because it blocks.  So it's a no-go
   block.  See, that's funny."
  []
  (loop [[data chosen-channel] (async/alts!! [(async/timeout (cf/*config* :channel-timeout)) spool-channel])
         file-map (files/make-empty-file-map cf/*config* (files/make-time-map))]
    (if (nil? data)
      (recur (async/alts!! [(async/timeout (cf/*config* :channel-timeout)) spool-channel])
             (files/rotate-file-map cf/*config* file-map))
      (let [new-file-map (files/write-json-to-file cf/*config* file-map data)]
        (recur (async/alts!! [(async/timeout (cf/*config* :channel-timeout)) spool-channel])
               (files/rotate-file-map cf/*config* new-file-map))))))


(defn read-carbon-line [line-mapping]
  "line-mapping is a map containing the keys :line and :client-info.
   The line is a whitespace-separated string that looks like this:

     metric-name value timestamp\n

   This is broken up into a vector of [string float float]

   The :socket is provided so that in case of an error, the error can be
   tracked to the remote host and port that provided the bad metric."
  (defn get-address []
    ((line-mapping :client-info) :address))
  (defn get-line []
    (s/trim (line-mapping :line)))

  (if-not (sanitize/validate-line (get-line))
    (warn "Received an bogus line: " (get-line) " from " (get-address))
    (let [metric-list (s/split (get-line) #"\s+")
          metric-name (get metric-list 0)
          value (get metric-list 1)
          timestamp (get metric-list 2)]
      (println (str "[metric-name value timestamp] is " metric-name " " value " " timestamp))
      (async/go (async/>! spool-channel
                          [metric-name value timestamp])))))

(async/go
 (while true (read-carbon-line (async/<! carbon-channel))))

;; based on the aleph example tcp service at https://github.com/ztellman/aleph/wiki/TCP
(defn carbon-receiver [ch client-info]
  (println ch)
  (lamina/receive-all ch
                      #(async/>!! carbon-channel { :line % :client-info client-info})))


;; From server-socket.
(defn on-thread [f]
  (doto (Thread. ^Runnable f)
    (.start)))

(defn -main [& args]
  (let [cmdline-args (carbon-relaj.cmdline/parse-args args)]
        ;; Run the writer on its own thread.
        (on-thread #(write-metric-to-file))
        ;; Debugging log
        (println "HERE")
        (aleph/start-tcp-server carbon-receiver {:port (cf/*config* :lineproto-port)
                                                 :frame (gloss.core/string :utf-8 :delimiters ["\n"])})))
