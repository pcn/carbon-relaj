(ns carbon-relaj.cmdline
  (:use [docopt.core :only [-docopt]]) ; import the -docopt function from docopt.core instead of the macro
  (:gen-class))

; The docopt macro breaks when not used in -main.  Annoying, but as
; long as there's a work-around in the function -docopt, it's OK with me.

(def docstring "carbon-relaj: A spooling carbon relay

Usage:
  carbon-relaj [options]

Options:
  -h --help             Show this screen.
  --version             Show version.
  -c --config=<path>    Path to the config file [default: /opt/graphite/conf/relaj.conf])
  -s --spooldir=<path>  Path to the spool directory [default: /opt/graphite/spool]")

(def relaj-version "0.0.1" )

(defn parse-args
  "Parse args if provided, otherwise parse *command-line-args* "
  ([]
     (parse-args *command-line-args*))

  ([args]
     (let [parsed (-docopt docstring args)]
;       (into {} (doto parsed (.put "--version" relaj-version))))))
       (doto parsed (.put "--version" relaj-version)))))

(def fake-help-cmdline ["foo" "-h" "-c" "/bla/blahblah"])

(defn test-help-args []
  (let [fake-cmdline ["foo" "-h"]]
    (parse-args fake-cmdline)))

(def test-config-cmdline ["foo" "-c" "/some/path/thing"])
