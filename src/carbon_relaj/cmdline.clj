(ns carbon-relaj.cmdline
  (:use [docopt.core :only [-docopt]]) ; import the -docopt function from docopt.core instead of the macro
  (:gen-class))

; The docopt macro breaks when not used in -main.  Annoying, but as
; long as there's a work-around in the function -docopt, it's OK with me.

(def docstring "carbon-relaj: A spooling carbon relay

Usage:
  carbon-relaj [options] <spool-directory>

Options:
  -h --help             Show this screen.
  --version             Show version.
  -c --config=<path>    Path to the config file [default: /opt/graphite/conf/relaj.conf]")

(def relaj-version "0.0.1" )


(defn parse-args 
  []
  "Parse *command-line-args*"
  (parse-args *command-line-args*)

  [args]
  "Parse the provided args"
  (let [parsed (-docopt docstring args)]
    (into (doto parsed (.put "--version" relaj-version)))))

(def fake-help-cmdline ["foo" "-h" "-c" "/bla/blahblah"])

(defn test-help-args []
  (let [fake-cmdline ["foo" "-h"]]
    (parse-args fake-cmdline)))

(defn test-config-cmdline ["foo" "-c" "/some/path/thing"]
