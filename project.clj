(defproject carbon-relaj "0.1.0-SNAPSHOT"
  :description "An asychronous carbon-relay that accepts messages from the network and spools to disk"
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [org.clojure/data.json "0.2.3"]
                 [org.clojure/core.async "0.1.242.0-44b1e3-alpha"]
                 [org.clojure/tools.logging "0.2.6"]
                 [fs "1.3.3"]
                 [clj-time "0.6.0"]
                 [me.raynes/fs "1.4.5"]
                 [com.cemerick/pomegranate "0.2.0"]
                 [aleph "0.3.0"]
                 [clojure-ini "0.0.1"]
                 [docopt "0.6.1"]]
  ; :main ^:skip-aot carbon-relaj.core
  :main carbon-relaj.core)
;   :target-path "target/%s"
;   :profiles {:uberjar {:aot :all}})
