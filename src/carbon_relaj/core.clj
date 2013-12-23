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
            [carbon-relaj.socket :as relay-socket]))

; First check that we're using a jvm that gives fs the ability to do
; hard links.
(if (< 1 (count (filter true? (clojure.core/map #(= "link" (str (first %)))
                                                (seq (ns-publics (the-ns 'me.raynes.fs)))))))
  (do
    (error "This jre doesn't provide you with the ability to do hard links.  Use java versions >=1.7.")
    (error "Exiting with a sad face.  :(")
    (System/exit 1)))

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


(defn exit-config-error
  "exits with a message, maybe an exit code"
  ([]
     (exit-config-error "An error was encountered, and the system will now exit.  Sorry it didn't work out." 1))
  ([message]
     (exit-config-error  message 1))
  ([message errorcode]
     (error message)
     (System/exit (int errorcode))))

;; XXX fix the checking of directories, etc.
;; (defn check-config [config]
;;   "Verify that configuration exists to the extent that it makes sense.
;; Try to keep this lightweight enough that it can be re-run at runtime
;; so that a cogent error message can be produced if an admin does
;; something while the system is running"
;;   ; Check directories
;;   (let [fail-directories (check-dirs-exist (config-map :target-list))]
;;     (if (> 0 (count(fail-directories)))
;;       (exit-config-error
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

   Each metric is a vector of [metric (value timestamp)]
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
  "line-mapping is a map containing the keys :line and :socket.
   The line is a whitespace-separated string that looks like this:

     metric-name value timestamp\\n

   This is broken up into a vector of [string float float]

   The :socket is provided so that in case of an error, the error can be
   tracked to the remote host and port that provided the bad metric."
  (if (empty? (line-mapping :line))
                                        ; TODO: detect more bad metric values
    (warn "Received an empty line from " (.getInetAddress (line-mapping :socket)))
    (let [splitup (clojure.string/split (line-mapping :line) #"\s+" 3)]
      (if (not= (count splitup) 3)
        (warn "Received an invalid line \"" (line-mapping :line) "\" from "
              (.getInetAddress (line-mapping :socket)))
        (do
          (async/go (async/>! spool-channel
                              [(splitup 0) (read-string (splitup 1))
                               (read-string (splitup 2))])))))))

(async/go
 (while true (read-carbon-line (async/<! carbon-channel))))


(defn carbon-receiver [config]
  "Listen on a port, and accept carbon line-protocol data.  For each connected socket
   when a line is read, feed the data to an async channel as a vector
   of [message (remote-address remote-port)] so that downstream functions
   can log information about the connection."
  (letfn [(carbon-spooler [in out socket]
            (binding [*in* (BufferedReader. (InputStreamReader. in))
                      *out* (OutputStreamWriter. out)]
              (loop []
                (let [input (read-line)]
                  (if-not (nil? input)
                    (do
                      (println input "\n")
                      (async/>!! carbon-channel { :line input :socket socket})))
                  (recur)))))]
    (relay-socket/create-server (config :listen-port) carbon-spooler)))


(defn -main []
 ; Run the writer on its own thread.
  (relay-socket/on-thread #(write-metric-to-file config-map))
  (println "HERE")
  (carbon-receiver config-map))
