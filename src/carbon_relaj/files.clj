(ns carbon-relaj.files
  (:gen-class)
  (:use [clojure.main :only (repl)]
        [clojure.tools.logging :only (debug info warn error)])
  (:require [clojure.core.async :as async]
            [me.raynes.fs :as fs]
            [clojure.data.json :as json]))

(defn link [src target]
  "The java createLink does things in the opposite order
   from the system call link(2).  What.  The.  Fuck."
  (fs/link target src))

(defn exists? [target]
  (fs/exists? target))

(defn make-time-map
  ([]
     (make-time-map (System/currentTimeMillis)))
  ([the-time]
     "the-time is a long int as returned by System/currentTimeMillis"
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
  (doall
   (for [new-dir (config :target-list)]
     (do
       ;; XXX add error checking here.
       ;; XXX and recovery from the inevitable errors.
       (let [current-name (f :file-name)
             base-name (fs/base-name current-name)
             new-name (str (config :send-dir) "/" new-dir "/" base-name)]
         (link current-name new-name)))))
  (fs/delete (f :file-name)))

(defn rotate-file-map [config file-map]
  "If a file-map is due for rotation, rotate it and return a new empty file map"
  (if (and (not= (file-map :file-name) "") (file-needs-rotation? config file-map))
    (do
      (relink-file-on-disk config file-map) ; XXX this isn't happening.
      (.close (file-map :writable-file))
      (make-empty-file-map config (make-time-map)))
    file-map))

(defn update-file-map [config file-map]
  "Call this to return a file map - either a new one if there isn't one,
   or the current one if it's in use"
  (if (nil? (file-map :writable-file))
    (let [the-time (make-time-map)
          this-thread (.getId (Thread/currentThread))
          new-file-name (make-file-name (config :temp-dir) this-thread the-time)
          new-writable-file (clojure.java.io/writer new-file-name)]
      (assoc-in (assoc-in (assoc-in file-map [:file-name] new-file-name)
                          [:the-time] the-time)
                [:writable-file] new-writable-file))
    file-map))

(defn write-json-to-file [config prior-file-map data]
  "This will write data to a file.  The file name contains the thread-id
   to allow for multiple writer threads in the future."
  (let [this-file-map (update-file-map config prior-file-map)]
    (.write (this-file-map :writable-file) (str (json/write-str data) "\n"))
    this-file-map))
