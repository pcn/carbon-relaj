;; initially based on http://stackoverflow.com/questions/1223352/writing-a-multiplexing-server-in-clojure
;; and https://github.com/cymen/clojure-socket-echo/blob/master/src/clojure_socket_echo/core.clj

(ns carbon-relaj.socket
  (:gen-class)
  (:import (java.net InetAddress ServerSocket Socket SocketException)
           (java.io InputStreamReader OutputStream
                    OutputStreamWriter PrintWriter
                    InputStreamReader BufferedReader)
           (clojure.lang LineNumberingPushbackReader))
  (:use [clojure.tools.logging :only (debug info warn error)]))

;; From server-socket.  TODO: consider putting this in another namespace.
(defn on-thread [f]
  (doto (Thread. ^Runnable f)
    (.start)))

(defn- close-socket [^Socket s]
  (when-not (.isClosed s)
    (doto s
      (.shutdownInput)
      (.shutdownOutput)
      (.close))))

(defn- accept-fn [^Socket s connections fun]
  "Provides fun the socket, so that it can access the socket info"
  (let [ins (.getInputStream s)
        outs (.getOutputStream s)]
    (on-thread #(do
                  (dosync (commute connections conj s))
                  (try
                    (fun ins outs s)
                    (catch SocketException e))
                  (close-socket s)
                  (dosync (commute connections disj s))))))

(defstruct server-def :carbon-socket :connections)

(defn- create-server-aux [fun ^ServerSocket ss]
  (let [connections (ref #{})]
    (on-thread #(when-not (.isClosed ss)
                  (try
                    (accept-fn (.accept ss) connections fun)
                    (catch SocketException e))
                  (recur)))
    (struct-map server-def :carbon-socket ss :connections connections)))

(defn create-server
  "Creates a server socket on port. Upon accept, a new thread is
created which calls:

 (fun input-stream output-stream)

Optional arguments support specifying a listen backlog and binding
to a specific endpoint."
  ([port fun backlog ^InetAddress bind-addr]
     (create-server-aux fun (ServerSocket. port backlog bind-addr)))
  ([port fun backlog]
     (create-server-aux fun (ServerSocket. port backlog)))
  ([port fun]
     (create-server-aux fun (ServerSocket. port))))

(defn close-server [server]
  (doseq [s @(:connections server)]
    (close-socket s))
  (dosync (ref-set (:connections server) #{}))
  (.close ^ServerSocket (:carbon-socket server)))

(defn connection-count [server]
  (count @(:connections server)))

;; End of the server-socket excerpt
