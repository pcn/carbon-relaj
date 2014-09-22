(ns carbon-relaj.channels
  (:require [clojure.core.async :as async]
            [carbon-relaj.config :as cf]))

;; Read configuration and command line, make cf/*config* available
(def carbon-channel (async/chan (get (cf/config) :channel-queue-size)))
                                        ; Channel for input of carbon line proto from the network.
(def spool-channel (async/chan (get (cf/config) :channel-queue-size)))
                                        ; Channel for metrics to head to disk.
