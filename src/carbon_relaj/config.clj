(ns carbon-relaj.config
  (:gen-class)
  (:require ;; [beckon]
            [clojure-ini.core :as ini]
            [carbon-relaj.cmdline]
            [carbon-relaj.files :as files]
            [carbon-relaj.util :as util]))


;; The equivalent of the default-config-map in a config file is:
;;
;; # Configuration for carbon-relaj
;;
;; [default]
;; lineproto-port          = 2003
;; channel-queue-size      = 128
;; channel-timeout         = 250
;; spool-dir               = "/opt/graphite/foo"
;; temp-dir                = "/tmp/foo/temp"
;; send-dir                = "/tmp/foo/send"
;; target-list             = "host-a" "host-b" "host-c" "host-d"
;; file-rotation-period-ms = 1000

(def default-config-map {
                         :lineproto-port 2003
                         :channel-queue-size 128
                         :channel-timeout 250
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
       ;; (println (format "Config-file is %s" config-file))
       (read-config config-file)))
  ([config-file-path]
     (if (files/exists? config-file-path)
       (into default-config-map (ini/read-ini config-file-path :keywordize true :comment-char \#))
       (util/exit-error (format "The configuration file %s failed to exist.  Exiting." config-file-path)))))

(defn current-config-map
  ([]
     "Reads config from defaults, and merges in command line and configs
      on disk."
     (current-config-map (read-config)))
  ([config-map]
     (current-config-map (keys config-map) (vals config-map)))
  ([key-list value-list]
     "Reads config from disk and overrides each key in key-list with the
      corresponding value from value-list"
     (let [new-config (read-config)]
        (into new-config
              (map hash-map key-list value-list)))))

(defn config []
  "Returns a map of the current configuration, without explicit defaults
as returned by current-config-map"
  (into {} (rest (current-config-map))))

;; (reset! (beckon/signal-atom "HUP") #{update-config})



; Configuration should be loaded at the outset.  Configuration should also be
; modifiable at run time - either via a manhole or via a signal, etc.
