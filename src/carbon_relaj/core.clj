;; initially based on http://stackoverflow.com/questions/1223352/writing-a-multiplexing-server-in-clojure
;; and https://github.com/cymen/clojure-socket-echo/blob/master/src/clojure_socket_echo/core.clj

(ns carbon-relaj.core
  ; (:use server.socket)
  (:gen-class)
  (:import (java.net InetAddress ServerSocket Socket SocketException)
           (java.io InputStreamReader OutputStream
                    OutputStreamWriter PrintWriter
                    InputStreamReader BufferedReader)
           (clojure.lang LineNumberingPushbackReader))
  (:use [clojure.main :only (repl)]
        [clojure.tools.logging :only (info warn error)])
  (:require [clojure.core.async :as async :refer :all]
            [clojure.java.io :as io ]
            [me.raynes.fs :as fs]
            [clojure.data.json :as json]))

; First check that we're using a jvm that gives fs the ability to do
; hard links.
(if (< 1 (count (filter true? (clojure.core/map #(= "link" (str (first %)))
                                                (seq (ns-publics (the-ns 'me.raynes.fs)))))))
  (do
    (error "This jdk doesn't provide you with the ability to do hard links.  Use java versions >=1.7.")
    (error "Exiting with a sad face.  :(")
    (System/exit 1)))

;; Configuration
;; XXX remove this to separate file later.
(def config-map {
                 :listen-port 8001
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
  (error "An error was encountered, and the system will now exit.  Sorry it didn't work out.")
  (System/exit 1))
  ([message]
  (error message)
  (System/exit 1))
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

;; From server-socket.  TODO: consider putting this in another namespace.
(defn- on-thread [f]
  (doto (Thread. ^Runnable f)
    (.start)))

(defn- close-socket [^Socket s]
  (when-not (.isClosed s)
    (doto s
      (.shutdownInput)
      (.shutdownOutput)
      (.close))))

(defn- accept-fn [^Socket s connections fun]
  "Provides fun the socket, so that it can access the socket info"
  (let [ins (.getInputStream s)
        outs (.getOutputStream s)]
    (on-thread #(do
                  (dosync (commute connections conj s))
                  (try
                    (fun ins outs s)
                    (catch SocketException e))
                  (close-socket s)
                  (dosync (commute connections disj s))))))

(defstruct server-def :carbon-socket :connections)

(defn- create-server-aux [fun ^ServerSocket ss]
  (let [connections (ref #{})]
    (on-thread #(when-not (.isClosed ss)
                  (try
                    (accept-fn (.accept ss) connections fun)
                    (catch SocketException e))
                  (recur)))
    (struct-map server-def :carbon-socket ss :connections connections)))

(defn create-server
  "Creates a server socket on port. Upon accept, a new thread is
created which calls:

(fun input-stream output-stream)

Optional arguments support specifying a listen backlog and binding
to a specific endpoint."
  ([port fun backlog ^InetAddress bind-addr]
     (create-server-aux fun (ServerSocket. port backlog bind-addr)))
  ([port fun backlog]
     (create-server-aux fun (ServerSocket. port backlog)))
  ([port fun]
     (create-server-aux fun (ServerSocket. port))))

(defn close-server [server]
  (doseq [s @(:connections server)]
    (close-socket s))
  (dosync (ref-set (:connections server) #{}))
  (.close ^ServerSocket (:carbon-socket server)))

(defn connection-count [server]
  (count @(:connections server)))

;; End of the server-socket excerpt

; TODO
; Create channels in echo-server.

; create a channel that will receive the feed
; output from the echo-server.
;
; Write that feed to disk.

;; Channels
(def carbon-channel (chan 1024)) ; Channel for input of carbon line proto from the network.
(def spool-channel (chan 1024)) ; Channel for metrics to head to disk.

(defn make-file-name [prefix time]
  (let [sec (quot time 1000) ms (mod time 1000)]
        (str prefix "/" sec "." (format "%03d" ms))))

(defn make-empty-file-map
  "The new spool file mapping, containing the info I need
to know about rotation time, etc.
"
  ([config the-time]
     (let [temp-dir (config :temp-dir)]
       {:writable-file nil
        :file-name ""
        :the-time the-time })))

(defn file-due-for-rotation [config file]
  (let [difference-in-ms (- (System/currentTimeMillis) (file :the-time))]
;    (println "Difference in ms is " difference-in-ms)
;    (println "So file-due-for-rotation is "     (< (config :file-rotation-period-ms) difference-in-ms))
    (< (config :file-rotation-period-ms) difference-in-ms)))

(defn rotate-file [config f]
  "links the file to a new file or set of filenames"
                                        ; Let's do this synchronously, could be a map, though
  (for [new-path (config :send-dir)]
    (let [base-name (fs/base-name (f :file-name))
          new-name (str "path" "/" base-name)]
      (println "Trying to rotate " (f :file-name) " to " new-name)
      (fs/link (f :file-name) new-name))))


(defn write-data-to-file [config file-ref-map data]
  "This will write data to a file.  Currently this is trying to be
super-safe in using reference types to prevent access to a file when
it may be closed. However, this function should only ever be used in a
single thread at the moment.  To address this, add the thread id to
the file map, make it part of the file name and we can have multiple
threads."
  (if (= (@file-ref-map :file-name) "") ; activate the writeable file if it's not there
    (let [the-time-ms (System/currentTimeMillis)
          new-file-name (make-file-name (config :temp-dir) the-time-ms)
          new-writable-file (clojure.java.io/writer new-file-name)]
      (dosync
       (alter file-ref-map assoc-in [:writable-file] new-writable-file)
       (alter file-ref-map assoc-in [:file-name] new-file-name)
       (alter file-ref-map assoc-in [:the-time] the-time-ms))))
;  (println (str "file-ref-map has " (seq @file-ref-map)))
;  (println (str "writable-file is " (@file-ref-map :writable-file)))
  (.write (@file-ref-map :writable-file) (str (json/write-str data) "\n")))

; Test with
; (>!! spool-channel ["a.b.c.d" (System/currentTimeMillis)  (System/currentTimeMillis) ])
(defn write-metric-to-file [config]
  "Pulls a metric off of the channel spool-channel which is in-scope.

Each metric is a vector of [metric value timestamp]
It will be written out as a 1-line json document in a file with
many such lines.  The 1-line document is a defensive measure since
the server has to accept a lot of possibly strange things on the wire.
Using json will encode it in a readable manner (in most cases).

This needs to be run on an independent thread that will read from an
async channel, and write to disk, rotating files as needed.  This
can't be called from a go block because it blocks.  It's a no-go
block.  See, that's funny.
"
  (def file-ref-dict (ref (make-empty-file-map config (System/currentTimeMillis))))
  (loop [[data chosen-channel] (alts!! [(timeout 250) spool-channel])] ; XXX make timeout tunable
;;     (if-not (nil? data)
;;       (do
;;         (println "Not nil: " data)
;;         (println (json/write-str data))
;;         (write-data-to-file config file-ref-dict data))
;;       (println "data was nil"))
    (if-not (nil? data)
      (write-data-to-file config file-ref-dict data))
    (if (and (not= (@file-ref-dict :file-name) "") (file-due-for-rotation config @file-ref-dict))
      (do
        (rotate-file @file-ref-dict config) ; XXX this isn't happening.
        (let [old-file (@file-ref-dict :writable-file)]
          (dosync
           (alter file-ref-dict assoc-in [:writable-file] nil)
           (alter file-ref-dict assoc-in [:file-name] ""))
          (.close old-file)))) ;; (println (str "file-ref-dict has been emptied and is now: " (seq @file-ref-dict)))))
    (recur (alts!! [(timeout 250) spool-channel]))))


(defn read-carbon-line [line-mapping]
  "line-mapping is a map containing the keys :line and :socket.
The line is a whitespace-separated string that looks like this:

  metric-name value timestamp

This is broken up into a vector of [string float float]

The :socket is provided so that in case of an error, the error can be
tracked to the remote host and port that provided the bad metric.
"
  ; (binding [*out* *err*]
  ;   (println "I'm going to get a line:" line))
  (if (empty? (line-mapping :line))
    (warn "Received an empty line from " (.getInetAddress (line-mapping :socket))) ; TODO: log an error about an invalid metric.
    (let [splitup (clojure.string/split (line-mapping :line) #"\s+" 3)]
      (if (not= (count splitup) 3)
        (warn "Received an invalid line \"" (line-mapping :line) "\" from " (.getInetAddress (line-mapping :socket)))
        (do
          ;; TODO: check the types of the value of metrics are not NaN inf etc.
          (go (>! spool-channel [(splitup 0) (read-string (splitup 1)) (read-string (splitup 2))])))))))


(go
 (while true (read-carbon-line (<! carbon-channel))))
; (go
;  (while true (write-to-carbon-file (<! spool-channel))))


(defn carbon-receiver []
  "Listen on a port, and accept carbon line-protocol data.  For each connected socket
when a line is read, feed the data to a clojure async channel as a vector
of [message (remote-address remote-port)] so that downstream functions
can log information about the connection.
"
  (letfn [(carbon-spooler [in out socket]
                    (binding [*in* (BufferedReader. (InputStreamReader. in))
                              *out* (OutputStreamWriter. out)]
                      (loop []
                        (let [input (read-line)]
                          (print input "\n")
                          (go (>! carbon-channel { :line input :socket socket}))
                          (flush))
                        (recur))))]
    (create-server 9090 carbon-spooler)))

; Run the writer on its own thread.
(on-thread #(write-metric-to-file config-map))

(defn -main []
  (carbon-receiver))
