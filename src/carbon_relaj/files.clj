(ns carbon-relaj.files
  (:gen-class)
  (:use [clojure.tools.logging :only (debug info warn error)])
  (:require [clojure.core.async :as async]
            [me.raynes.fs :as fs]
            [clojure.data.json :as json]))

(defn link [src target]
  "The java createLink does things in the opposite order from the unix
system call link(2).  What.  The.  Fuck.  The function name is halfway
between the unix system call link(), but its argument order follows the
windows CreateHardLink call.  Java devs, please get a designated driver
before you implement another API"
  (fs/link target src))

(defn exists? [target]
  (fs/exists? target))

(defn make-time-map
  "If a time (in milliseconds since the epoch) isn't provided, use the
current time.  If provided, it's a long int as returned by
System/currentTimeMillis"
  ([]
     (make-time-map (System/currentTimeMillis)))
  ([the-time]
  (let [t the-time, seconds (quot t 1000), ms (mod t 1000), float-val (/ (float t) 1000)]
    {:long t, :seconds seconds, :ms ms, :float float-val })))

(defn make-file-name [prefix thread-id time]
  "Create a file based on the id of the thread, and the current time "
  (let [sec (time :seconds) ms (time :ms)]
    (str prefix "/" thread-id "-" sec "." (format "%03d" ms))))

(defn make-empty-file-map
  "The new spool file mapping, containing the info I need
   to know about rotation time, etc."
  ([config the-time]
     (let [temp-dir (config :temp-dir)]
       {:writable-file nil
        :file-name ""
        :the-time the-time })))


(defn file-needs-rotation? [config file]
  (let [difference-in-ms (- (System/currentTimeMillis) ((file :the-time) :long))]
    (< (config :file-rotation-period-ms) difference-in-ms)))

(defn relink-file-on-disk [config f]
  "links the file to a new file or set of filenames"
  (debug "I think I should be acting on this: " config)
  (let [current-name (f :file-name)]
    (doall
     (for [new-dir (config :target-list)]
       (do
         ;; XXX add error checking here.
         ;; XXX and recovery from the inevitable errors.
         (let  [base-name (fs/base-name current-name)
                new-name (str (config :send-dir) "/" new-dir "/" base-name)]
           (link current-name new-name)))))))

(defn rotate-file-map [config file-map]
  "If a file-map is due for rotation, rotate it, delete the
now-obsoleted file and return a new empty file map"
  (if (and (not= (file-map :file-name) "") (file-needs-rotation? config file-map))
    (do
      (relink-file-on-disk config file-map)
      (.close (file-map :writable-file))
      (fs/delete (file-map :file-name))
      (make-empty-file-map config (make-time-map)))
    file-map))

(defn update-file-map [config file-map]
  "Call this to return a file map - either a new one if there isn't one,
   or the current one if it's in use"
  (debug config)

  (if (nil? (file-map :writable-file))
    (let [the-time (make-time-map)
          this-thread-id (.getId (Thread/currentThread))
          new-file-name (make-file-name (config :temp-dir) this-thread-id the-time)
          new-writable-file (clojure.java.io/writer new-file-name)]
      (print "The temp dir name is ")
      (println new-file-name)
      (assoc-in
       (assoc-in
        (assoc-in file-map [:file-name] new-file-name)
        [:the-time] the-time)
       [:writable-file] new-writable-file))
    file-map))

(defn write-json-to-file [config prior-file-map data]
  "This will write data to a file.  The file name contains the thread-id
   to allow for multiple writer threads in the future."
  (let [this-file-map (update-file-map config prior-file-map)]
    (.write (this-file-map :writable-file) (str (json/write-str data) "\n"))
    this-file-map))
