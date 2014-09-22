(defproject carbon-relaj "0.1.0-SNAPSHOT"
  :description "An asychronous carbon-relay that accepts messages from the network and spools to disk"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 ;; [org.clojure/clojure "1.7.0-alpha2"]
                 [org.clojure/data.json "0.2.3"]
                 [org.clojure/core.async "0.1.242.0-44b1e3-alpha"]
                 [org.clojure/tools.logging "0.2.6"]
;;                  [fs "1.3.3"] ;; Older fs module, change out for raynes/fs
                 [clj-time "0.8.0"]                 ;; Time
                 [me.raynes/fs "1.4.5"]             ;; Simplify fs work
;;                 [com.cemerick/pomegranate "0.2.0"] ;; XXX I don't think I need this
                 [aleph "0.3.3"]                    ;; Threaded async network handling
                 [clojure-ini "0.0.2"]              ;; Config file parser
                 [docopt "0.6.1"]                   ;; Command line parser
;;                  [beckon "0.1.1"]                   ;; Posix signal handling
;;                 [interval-metrics "1.0.0"]         ;; aleph
                 [com.taoensso/timbre "3.0.1"]      ;; Use this to do profiling to send data to interval-metrics
                 [bouncer "0.3.1-beta1"]]           ;; We're getting data from the network, need a validation system
 :main ^:skip-aot carbon-relaj.core
;;   :main carbon-relaj.core
 )
;;   :target-path "target/%s"
;;   :profiles {:uberjar {:aot :all}})
