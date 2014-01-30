(ns carbon-relaj.config
  (:gen-class)
  (:require [beckon]
            [clojure-ini.core :as ini]))


(def default-config-map {
                 :lineproto-port 2003
                 :channel-queue-size 128
                 :spool-dir "/tmp/foo"
                 :temp-dir "/tmp/foo/temp"
                 :send-dir "/tmp/foo/send"
                 :target-list ["host-a", "host-b", "host-c", "host-d"]
                 :file-rotation-period-ms 1000})


(defn read-config
  ([]
     (read-config (carbon-relaj.cmdline/parse-args *command-line-args*)))

  ([config-file-path]
     "Read and parse the config file.
   TODO: check file existence, etc"
     (ini/read-ini config-file-path :keywordize true :comment-char "#")))

; XXX I don't think this needs to be an atom.  If it happens
; to be closed over in some scope, though, this may not have
; an affect.  So it probably is better to make this an atom.
(def *config* (read-config))

(defn update-config-map
  []
  "Reads config from disk and updates it."

  [key-list value-list]
  "Reads config from disk and overides each key in key-list
   with the corresponding value from value-list"

(defn update-config
  (set! *config* (update-config-map)))


(reset! (beckon/signal-atom "HUP") #{read-config})



; Configuration should be loaded at the outset.  Configuration should also be
; modifiable at run time - either via a manhole or via a signal, etc.

; First step in config checking.
;; XXX this was stopped mid-way
;; (defn check-all-conf-directories [spool-dir temp-dir send-dir target-list]
;;   "Check the spool dir, temp-dir and send-dir for existence.  Then
;; check that there is an existing directory for each target"
;;   (let [dir-vector (concat spool-dir tmp-dir send-dir (for [d [target-list]]
;;                                                         :let qualified (format "%s/%s" d
;; (defn check-dirs-exist [directory-vec]
;;   "If a spool dir doesn't exist, put it into a map of those that
;; failed to be there for us.  Expects a sequence of directory strings.
;;
;; Returns: {directory-name boolean, directory-name boolean}
;; etc.
;; "
;;   (let [bad-dir-list (map #(conj []
;;                                  (str %)
;;                                  (str (.isDirectory (clojure.java.io/as-file %)))) directory-vec)]
;;     (into {} (filter #(= "false" (second %)) (check-all-conf-dirs directory-vec)))))



;; XXX fix the checking of directories, etc.
;; (defn check-config [config]
;;   "Verify that configuration exists to the extent that it makes sense.
;; Try to keep this lightweight enough that it can be re-run at runtime
;; so that a cogent error message can be produced if an admin does
;; something while the system is running"
;;   ; Check directories
;;   (let [fail-directories (check-dirs-exist (config-map :target-list))]
;;     (if (> 0 (count(fail-directories)))
;;       (util/exit-error
;;        "These directories failed %s"
;;        (for [f foo] (format "%s: %s\n" (first f) (second f)))) 3))
;;   ;; XXX TODO More checks here.
;; )

; End config checking
