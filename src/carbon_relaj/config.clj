(ns carbon-relaj.config
  (:gen-class)
  (:require [beckon]
            [com.brainbot.iniconfig :as ini]
            [carbon-relaj.cmdline]
            [carbon-relaj.files :as files]
            [carbon-relaj.util :as util]))


;; The default would be expressed in a config file as
;; [relaj]
;; lineproto-port = 2003
;; channel-queue-size = 128
;; channel-timeout = 250
;; spool-dir = /opt/graphite/spool
;; temp-dir = /opt/graphite/spool/temp
;; send-dir = /opt/graphite/spool/send
;; target-list = host-a,host-b,host-c,host-d
;; file-rotation-period-ms = 1000

;; Config is parsed as strings, we need to convert those
(def config-numbers [
                     "channel-queue-size"
                     "channel-timeout"
                     "file-rotation-period-ms"
                     "lineproto-port"])
(def config-lists ["target-list"])
(def default-config-map {
                         "lineproto-port" 2003
                         "channel-queue-size" 128
                         "channel-timeout" 250
                         "spool-dir" "/tmp/foo"
                         "temp-dir" "/tmp/foo/temp"
                         "send-dir" "/tmp/foo/send"
                         ;; The following is a string to match
                         ;; the results of evaluating a confg file.
                         ;; read-config will turn it into a proper iterable.
                         "target-list" "host-a,host-b,host-c,host-d"
                         "file-rotation-period-ms" 1000})


(defn parse-int [maybe-a-string]
  (println "Trying the number " maybe-a-string)
  (cond
   (number? maybe-a-string) maybe-a-string
   (nil? maybe-a-string) 0
   :else (Integer/parseInt maybe-a-string)))

(defn fix-config-types
  "All config types are read from disk as strings.  When they're
used, we need them to be other things - so far numbers and lists.
Convert those keys we know need to be convered here.  Note that this
currently doesn't allow for the result of one transformation to be used
in a subsequent transformation"
  [config]
  (-> config
      (into (for [k config-numbers] [k (parse-int (config k))]))
      (into (for [k config-lists] [k (clojure.string/split (config k) #"\s*,\s*")]))))

(defn read-config
  "Read and parse the config file. When the config-file-path is provided,
read in the config file if it exists, and merge its contents with
the defaults.
"
  ([]
     (let [config-file (get (carbon-relaj.cmdline/parse-args) "--config") ]
       (read-config config-file)))
  ([config-file-path]
     (if (files/exists? config-file-path)
       (do
         ;; Wow, if the vector of strings to update from contains
         ;; an error, the actual stack trace that comes back is extremely
         ;; hard to understand!
         (let [config-from-file (get (ini/read-ini config-file-path) "relaj")]
           (fix-config-types config-from-file)))
       (do
         (util/exit-error (format "The configuration file %s failed to exist.  Exiting." config-file-path))))))

; XXX I don't think this needs to be an atom.  If it happens
; to be closed over in some scope, though, this may not have
; any effect.  So it is worth verifying what I will do with this in
; order to determine whether this needs to be an atom.

;; TODO: update config from disk
(def ^:dynamic *config* default-config-map)


(defn update-config-map
  "[]
Reads config from disk and updates it.

[key-list value-list]
Reads config from disk and overrides each key in key-list with the
corresponding value from value-list"
  ([]
     (update-config-map  [] []))

  ([key-list value-list]
     (let [new-config (read-config)]
       (into new-config (map vector key-list value-list)))))

(def ^:dynamic *config* (update-config-map))

(defn update-config []
  (.bindRoot #'*config* (update-config-map)))


(reset! (beckon/signal-atom "HUP") #{update-config})


; Configuration should be loaded at the outset.  Configuration should also be
; modifiable at run time - either via a manhole or via a signal, etc.
