(ns carbon-relaj.write
  (:require [clojure.core.async :as async]
            [carbon-relaj.files :as files]
            [carbon-relaj.config :as cf]
            [carbon-relaj.channels :as chan]
            [carbon-relaj.util :as util]))
;; was write-metric-to-file
(defn transport-metric-to-write-channel
  "Pulls a metric off of the async channel.

   Each metric is a vector of [metric value timestamp]
   It will be written out as a 1-line json document in a file with
   many such lines.  The 1-line document is a defensive measure since
   the server has to accept a lot of possibly strange things on the wire.
   Using json will encode it in a way that's safely readable.

   This needs to be run on an independent thread that will read from an
   async channel, and write to disk, rotating files as needed.  This
   can't be called from a go block because it blocks.  So it's a no-go
   block.  See, that's funny."
  []
  (let [local-cf (cf/config)
        cf-timeout (local-cf :channel-timeout)
        spool chan/spool-channel
        writer files/write-json-to-file
        new-file-map files/make-empty-file-map
        rotate files/rotate-file-map]

    (loop [[data chan] (async/alts!! [(async/timeout cf-timeout) spool])
           file-map (new-file-map local-cf (files/make-time-map))]
      (recur (async/alts!! [(async/timeout cf-timeout) spool])
             (rotate local-cf (if (nil? data)
                                file-map
                                (writer local-cf file-map data)))))))

;; I want errors to come from this module, so start with a new thread
;; Run the writer on its own thread.
(defn thread-write-metrics
  "Write metrics from a channel to a rotating file, on an independent thread"
  []
  (util/on-thread #(transport-metric-to-write-channel)))
