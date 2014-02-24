(ns carbon-relaj.core
  (:gen-class :main true)
  (:use [clojure.main :only (repl)])
  (:require [clojure.core.async    :as async]
            [carbon-relaj.files    :as files]
            [carbon-relaj.util     :as util]
            [carbon-relaj.config   :as cf]
            [carbon-relaj.sanitize :as sanitize]
            [lamina.core           :as lamina]
            [aleph.tcp             :as aleph]
            [gloss.core            :as gloss]
            [taoensso.timbre       :as timbre]
            [clojure.string        :as s]
            [clojure.data.json     :as json]))


;; Initialization and sanity checks
(util/check-jvm-version)

;; Provides useful Timbre aliases in this ns - maybe move to conf since
;; it may e.g. get log-level changed at runtime?
(timbre/refer-timbre)

;; Read configuration and command line, make cf/*config* available
(cf/update-config)

;; Channels
(def carbon-channel (async/chan (cf/*config* "channel-queue-size"))) ; Channel for input of carbon line proto from the network.
(def json-spool-channel (async/chan (cf/*config* "channel-queue-size"))) ; Channel for metrics to head to disk.


;; Test with
;; (-main) (async/>!! json-spool-channel ["a.b.c.d" ((files/make-time-map) :float)  ((files/make-time-map) :float)])
(defn write-metric-to-file
  "Pulls a metric off of the channel json-spool-channel.

   Each metric is a vector of [metric value timestamp]
   It will be written out as a 1-line json document in a file with
   many such lines.  The 1-line document is a defensive measure since
   the server has to accept a lot of possibly strange things on the wire.
   Using json will encode it in a way that's safely readable.

   This needs to be run on an independent thread that will read from an
   async channel, and write to disk, rotating files as needed.  This
   can't be called from a go block because it blocks.  So it's a no-go
   block.  See, that's funny."
  []
  ;; XXX binding the channel-timeout here may mean that it can't be re-configured.
  (defn timeout_ [conf]
    (conf "channel-timeout"))

  (loop [[json-data chosen-channel] (async/alts!! [(async/timeout (timeout_ cf/*config*)) json-spool-channel])
         file-map (files/make-empty-file-map cf/*config* (files/make-time-map))]
    (if (nil? json-data)
      (recur (async/alts!! [(async/timeout (timeout_ cf/*config*)) json-spool-channel])
               (files/rotate-file-map file-map))
        (let [new-file-map (files/write-json-to-file cf/*config* file-map json-data)]
          (recur (async/alts!! [(async/timeout (timeout_ cf/*config*)) json-spool-channel])
                 (files/rotate-file-map new-file-map))))))


(defn read-carbon-line [line-mapping]
  "line-mapping is a map containing the keys :line and :client-info.
   The line is a whitespace-separated string that looks like this:

     metric-name value timestamp\n

   This is broken up into a vector of [string float float] then
   returned as a newline-terminated string with a one-metric json document, e.g.
   \"[foo.metric.name 12345 1393226609]\\n\"

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
          timestamp (get metric-list 2)
          json-value (json/write-str [metric-name value timestamp])]
      ;; (println (str "[metric-name value timestamp] is " metric-name " " value " " timestamp))
      ;;      (async/go (async/>! spool-channel
      ;;                    [metric-name value timestamp] ))
      (async/go (async/>!! json-spool-channel (str json-value "\n"))))))

(async/go
 (while true (read-carbon-line (async/<!! carbon-channel))))

;; based on the aleph example tcp service at https://github.com/ztellman/aleph/wiki/TCP
(defn carbon-receiver [ch client-info]
  ;; (println ch)
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
        (println "HERE") ;; XXX convert this into an INFO message
        (aleph/start-tcp-server carbon-receiver {:port (cf/*config* "lineproto-port")
                                                 :frame (gloss.core/string :utf-8 :delimiters ["\n"])})))
