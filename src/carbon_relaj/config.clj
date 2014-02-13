(ns carbon-relaj.config
  (:gen-class)
  (:require [beckon]
            [clojure-ini.core :as ini]
            [carbon-relaj.cmdline]
            [carbon-relaj.files :as files]
            [carbon-relaj.util :as util]))


(def default-config-map {
                 :lineproto-port 2003
                 :channel-queue-size 128
                 :spool-dir "/tmp/foo"
                 :temp-dir "/tmp/foo/temp"
                 :send-dir "/tmp/foo/send"
                 :target-list ["host-a", "host-b", "host-c", "host-d"]
                 :file-rotation-period-ms 1000})


(defn read-config
  "Read and parse the config file. When the config-file-path is provided,
read in the config file if it exists, and merge its contents with
the defaults.
"
  ([]
     (let [config-file (get (carbon-relaj.cmdline/parse-args) "--config") ]
       (println (format "Config-file is %s" config-file))
       (read-config config-file)))
  ([config-file-path]
     (if (files/exists? config-file-path)
       (into default-config-map (ini/read-ini config-file-path :keywordize true :comment-char "#"))
       (util/exit-error (format "The configuration file %s failed to exist.  Exiting." config-file-path)))))

; XXX I don't think this needs to be an atom.  If it happens
; to be closed over in some scope, though, this may not have
; any effect.  So it is worth verifying what I will do with this in
; order to determine whether this needs to be an atom.

;; TODO: update config from disk
; (def ^:dynamic *config* default-config-map)



(defn update-config-map
  ([]
     "Reads config from disk and updates it."
     (update-config-map  [] []))

  ([key-list value-list]
     "Reads config from disk and overrides each key in key-list with the
      corresponding value from value-list"
     (let [new-config (read-config)]
       (into new-config (map vector key-list value-list)))))

(def ^:dynamic *config* (update-config-map))

(defn update-config []
  (alter-var-root (var *config*) (update-config-map)))

(reset! (beckon/signal-atom "HUP") #{update-config})


; Configuration should be loaded at the outset.  Configuration should also be
; modifiable at run time - either via a manhole or via a signal, etc.
