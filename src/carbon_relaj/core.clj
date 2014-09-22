(ns carbon-relaj.core
  (:gen-class :main true)
  (:use [clojure.main :only (repl)])
  (:require [clojure.core.async :as async]
            [carbon-relaj.files :as files]
            [carbon-relaj.util :as util]
            [carbon-relaj.config :as cf]
            [carbon-relaj.sanitize :as sanitize]
            [carbon-relaj.listener :as listener]
            [carbon-relaj.write :as wr]
            [lamina.core :as lamina]
            [aleph.tcp :as aleph]
            [gloss.core :as gloss]
            [taoensso.timbre :as timbre]
            [clojure.string :as s]))


;; Initialization and sanity checks
(util/check-jvm-version)

;; Provides useful Timbre aliases in this ns

;; XXX: move to conf since it may e.g. get log-level changed at runtime
(timbre/refer-timbre)

;; Read configuration and command line, make cf/*config* available
;; (cf/read-config)
(def carbon-channel (async/chan (get (cf/config) :channel-queue-size)))
                                        ; Channel for input of carbon line proto from the network.
(def spool-channel (async/chan (get (cf/config) :channel-queue-size)))
                                        ; Channel for metrics to head to disk.

;; ;; Test with
;; ;; (-main) (async/>!! spool-channel ["a.b.c.d" ((files/make-time-map) :float)  ((files/make-time-map) :float)])
;; From server-socket.
(defn on-thread [f]
  (doto (Thread. ^Runnable f)
    (.start)))

(defn -main [& args]
  (let [cmdline-args (carbon-relaj.cmdline/parse-args args)
        local-config (cf/config)]
    ;; (on-thread #(transport-metric-to-write-channel))
    ;; Debugging log
    (println "HERE") ;; XXX convert this into an INFO message
    (println local-config)
    (write/thread-write-metrics)
    (listener/receiver (cf/config))))
