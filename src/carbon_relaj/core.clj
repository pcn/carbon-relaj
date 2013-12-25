(ns carbon-relaj.core
  (:gen-class)
  (:import (java.net InetAddress ServerSocket Socket SocketException)
           (java.io InputStreamReader OutputStream
                    OutputStreamWriter PrintWriter
                    InputStreamReader BufferedReader)
           (clojure.lang LineNumberingPushbackReader))
  (:use [clojure.main :only (repl)]
        [clojure.tools.logging :only (debug info warn error)])
  (:require [clojure.core.async :as async]
            [carbon-relaj.files :as files]
            [carbon-relaj.util :as util]
            [lamina.core :as lamina]
            [aleph.tcp :as aleph]
            [gloss.core :as gloss]))


; First check that we're using a jvm that gives fs the ability to do
; hard links.
(if (< 1 (count (filter true? (clojure.core/map #(= "link" (str (first %)))
                                                (seq (ns-publics (the-ns 'me.raynes.fs)))))))
  (util/exit-error
   (str "This jre doesn't provide you with the ability to do hard links.  Use java versions >=1.7.\n"
        "Exiting with a sad face.  :(\n")
   100))


;; Configuration
;; XXX remove this to separate file later.
;; Test setup:
"
for f in temp send ; do
  for host in a b c d  ; do
    mkdir -p /tmp/foo/$f/host-$host
  done
done
"
(def config-map {
                 :listen-port 9090
                 :channel-queue-size 128
                 :spool-dir "/tmp/foo"
                 :temp-dir "/tmp/foo/temp"
                 :send-dir "/tmp/foo/send"
                 :target-list ["host-a", "host-b", "host-c", "host-d"]
                 :file-rotation-period-ms 1000})


; First step in config checking.
;; XXX this was stopped mid-way
;; (defn check-all-conf-directories [spool-dir temp-dir send-dir target-list]
;;   "Check the spool dir, temp-dir and send-dir for existence.  Then
;; check that there is an existing directory for each target"
;;   (let [dir-vector (concat spool-dir tmp-dir send-dir (for [d [target-list]]
;;                                                         :let qualified (format "%s/%s" d
;; (defn check-dirs-exist [directory-vec]
;;   "If a spool dir doesn't exist, put it into a map of those that
;; failed to be there for us.  Expects a sequence of directory strings.
;;
;; Returns: {directory-name boolean, directory-name boolean}
;; etc.
;; "
;;   (let [bad-dir-list (map #(conj []
;;                                  (str %)
;;                                  (str (.isDirectory (clojure.java.io/as-file %)))) directory-vec)]
;;     (into {} (filter #(= "false" (second %)) (check-all-conf-dirs directory-vec)))))



;; XXX fix the checking of directories, etc.
;; (defn check-config [config]
;;   "Verify that configuration exists to the extent that it makes sense.
;; Try to keep this lightweight enough that it can be re-run at runtime
;; so that a cogent error message can be produced if an admin does
;; something while the system is running"
;;   ; Check directories
;;   (let [fail-directories (check-dirs-exist (config-map :target-list))]
;;     (if (> 0 (count(fail-directories)))
;;       (util/exit-error
;;        "These directories failed %s"
;;        (for [f foo] (format "%s: %s\n" (first f) (second f)))) 3))
;;   ;; XXX TODO More checks here.
;; )

; End config checking

; TODO
; Write that feed to disk.

;; Channels
(def carbon-channel (async/chan (config-map :channel-queue-size))) ; Channel for input of carbon line proto from the network.
(def spool-channel (async/chan (config-map :channel-queue-size))) ; Channel for metrics to head to disk.


; Test with
; (-main) (async/>!! spool-channel ["a.b.c.d" ((files/make-time-map) :float)  ((files/make-time-map) :float)])
(defn write-metric-to-file [config]
  "Pulls a metric off of the channel spool-channel which is in-scope.

   Each metric is a vector of [metric value timestamp]
   It will be written out as a 1-line json document in a file with
   many such lines.  The 1-line document is a defensive measure since
   the server has to accept a lot of possibly strange things on the wire.
   Using json will encode it in a readable manner (in most cases).

   This needs to be run on an independent thread that will read from an
   async channel, and write to disk, rotating files as needed.  This
   can't be called from a go block because it blocks.  So it's a no-go
   block.  See, that's funny."
                                        ; XXX make timeout configurable
  (loop [[data chosen-channel] (async/alts!! [(async/timeout 250) spool-channel])
         file-map (files/make-empty-file-map config (files/make-time-map))]
    (if (nil? data)
      (recur (async/alts!! [(async/timeout 250) spool-channel])
             (files/rotate-file-map config file-map))
      (let [new-file-map (files/write-json-to-file config file-map data)]
        (recur (async/alts!! [(async/timeout 250) spool-channel])
               (files/rotate-file-map config new-file-map))))))


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
    (clojure.string/trim-newline (line-mapping :line)))
  (if (empty? (line-mapping :line))
                                        ; TODO: detect more bad metric values
    (warn "Received an empty line from " (get-address))
    (try
      (let [splitup (try (clojure.string/split (get-line) #"\s+" 3) (catch java.lang.IndexOutOfBoundsException e))
            metric-name (try (splitup 0) (catch java.lang.IllegalArgumentException e))
            value (try (Double/parseDouble (splitup 1)) (catch java.lang.IllegalArgumentException e))
            timestamp (try (Double/parseDouble (splitup 2)) (catch java.lang.IllegalArgumentException e))]
        (if (or (not= (count splitup) 3)
                (not (number? value))
                (not (number? timestamp)))
          (warn "Received an invalid line \""(get-line)"\" from " (get-address))
          (do
            (println (str "[metric-name value timestamp] is " metric-name " " value " " timestamp))
            (async/go (async/>! spool-channel
                                [metric-name value timestamp])))))
      (catch java.lang.IndexOutOfBoundsException e (warn "Received a bad line "(get-line)" from "(get-address))))))

(async/go
 (while true (read-carbon-line (async/<! carbon-channel))))

; based on the aleph example tcp service at https://github.com/ztellman/aleph/wiki/TCP
(defn carbon-receiver [ch client-info]
  (lamina/receive-all ch
               #(async/>!! carbon-channel { :line % :client-info client-info})))


;; From server-socket.
(defn on-thread [f]
  (doto (Thread. ^Runnable f)
    (.start)))

(defn -main []
 ; Run the writer on its own thread.
  (on-thread #(write-metric-to-file config-map))
  (println "HERE")
  (aleph/start-tcp-server carbon-receiver {:port (config-map :listen-port)
                                     :frame (gloss.core/string :utf-8 :delimiters ["\n"])}))
