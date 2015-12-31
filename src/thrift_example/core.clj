;; Copyright (c) 2015, Claudemiro Alves Feitosa Neto

;; Permission to use, copy, modify, and/or distribute this software for any
;; purpose with or without fee is hereby granted, provided that the above
;; copyright notice and this permission notice appear in all copies.

;; THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
;; WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
;; MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
;; ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
;; WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
;; ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
;; OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

(ns thrift-example.core
  (:import [thrift_example Calculator$Iface Calculator$Client Calculator$Processor]
           [org.apache.thrift.transport THttpClient TTransportException]
           [org.apache.thrift.protocol TJSONProtocol TJSONProtocol$Factory TCompactProtocol TCompactProtocol$Factory])
  (:require [compojure.core :refer :all]
            [compojure.route :as route]
            [ring.middleware.defaults :refer [wrap-defaults api-defaults]]
            [ring.middleware.reload :as reload]
            [ring.adapter.jetty :refer :all])
  (:gen-class))


(defn THRIFT
  "Implementation of the Thrift Servlet with compojure"
  ([path in-out-pfactory tprocessor] (THRIFT path in-out-pfactory in-out-pfactory tprocessor))
  ([path in-pfactory out-pfactory tprocessor]
   (ANY path {body :body} ;; body is an InputStream
     (let [in body
           out (java.io.ByteArrayOutputStream.)
           transport (org.apache.thrift.transport.TIOStreamTransport. in out)
           processor tprocessor
           in-pfactory in-pfactory
           out-pfactory out-pfactory]
       (.process processor
                 (.getProtocol in-pfactory transport)
                 (.getProtocol out-pfactory transport))
       (.flush out)
       (-> (ring.util.response/response (clojure.java.io/input-stream (.toByteArray out)))
           (ring.util.response/content-type "application/x-thrift"))))))


(def port (Integer/parseInt (or (System/getenv "PORT") "8000")))

(def processor (Calculator$Processor.
                (reify Calculator$Iface
                  (sum [this a b]
                    (+ a b)))))

(def transport (THttpClient. (str "http://localhost:" port "/calc/")))
(def protocol (TCompactProtocol. transport))
(def client (Calculator$Client. protocol))

(defroutes app-routes
  (GET "/" [] "Hello World")
  (THRIFT "/calc/" (TCompactProtocol$Factory.) processor)
  (GET "/sum/:n1/:n2" [n1 n2]
    (try
      (str (.sum client (Long/parseLong n1) (Long/parseLong n2)))
      (catch TTransportException e
        (do
          (str e)))))
  (route/not-found "Not found"))

(defn -main [& args]
  (let [handler (reload/wrap-reload #'app-routes)
        port port]
    (println (str "Running server on port " port " :)"))
    (run-jetty handler {:port port})))

