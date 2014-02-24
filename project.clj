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
                 [com.brainbot/iniconfig "0.2.0"]
                 [docopt "0.6.1"]
                 [beckon "0.1.1"]
                 [interval-metrics "1.0.0"]
                 [com.taoensso/timbre "3.0.1"] ; Use this to do profiling, maybe to send data to interval-metrics?
                 [bouncer "0.3.1-beta1"]]
  :jvm-opts ["-Xmx1024m"
             "-server"
             "-XX:+UseConcMarkSweepGC"
             "-XX:NewSize=768m"
             "-XX:MaxNewSize=768m"
             "-verbose:gc"
             "-XX:+PrintGCDetails"
             ]

  ;; :main ^:skip-aot carbon-relaj.core
  :main carbon-relaj.core)
;   :target-path "target/%s"
;   :profiles {:uberjar {:aot :all}})
