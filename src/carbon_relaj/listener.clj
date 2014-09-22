(ns carbon-relaj.listener
  (:require [clojure.core.async :as async]
            [carbon-relaj.util :as util]
            [carbon-relaj.channels :as chan]
            [carbon-relaj.config :as cf]
            [carbon-relaj.sanitize :as sanitize]
            [carbon-relaj.lineproto :as lineproto]
            [lamina.core :as lamina]
            [aleph.tcp :as aleph]
            [gloss.core :as gloss]))

(async/go
  (while true
    (lineproto/read-carbon-line (async/<! chan/carbon-channel))))

;; based on the aleph example tcp service at
;; https://github.com/ztellman/aleph/wiki/TCP
(defn carbon-receiver [ch client-info]
  "Receives a line and places it onto the carbon-channel, along with
identifying client-info"
  ;; (println ch)
  (lamina/receive-all ch
                      #(async/>!! chan/carbon-channel
                                  { :line % :client-info client-info})))

(defn receiver [config]
  (aleph/start-tcp-server carbon-receiver
                          {:port (get (cf/config) :lineproto-port)
                           :frame (gloss.core/string :utf-8 :delimiters ["\n"])}))
