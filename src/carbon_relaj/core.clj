(ns carbon-relaj.core
  (:gen-class :main true)
  (:use [clojure.main :only (repl)])
  (:require [carbon-relaj.util :as util]
            [carbon-relaj.config :as cf]
            [carbon-relaj.listener :as listener]
            [carbon-relaj.write :as write]
            [taoensso.timbre :as timbre]))

(util/check-jvm-version)

;; Provides useful Timbre aliases in this ns
;; XXX: move to conf since it may e.g. get log-level changed at runtime
(timbre/refer-timbre)

;; ;; Test with
;; ;; (-main) (async/>!! spool-channel ["a.b.c.d" ((files/make-time-map) :float)  ((files/make-time-map) :float)])
(defn -main [& args]
  (let [cmdline-args (carbon-relaj.cmdline/parse-args args)
        local-config (cf/config)]
    ;; (on-thread #(transport-metric-to-write-channel))
    ;; Debugging log
    (info "Start the writer thread")
    (write/thread-write-metrics)
    (info "Start the listener -> channel<")
    (listener/receiver (cf/config))))
