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

(defproject thrift-example "0.1.0-SNAPSHOT"
  :description "Example of Implementation of the Thrift Servlet with Clojure Ring/compojure"
  :url "https://github.com/dimiro1/thrift-clojure-example"
  :license {:name "ISC License"
            :url "http://www.isc.org/downloads/software-support-policy/isc-license/"}
  :dependencies [[org.clojure/clojure "1.7.0"]
                 [compojure "1.4.0"]
                 [ring "1.4.0"]
                 [ring/ring-defaults "0.1.5"]
                 [org.apache.thrift/libthrift "0.9.3"]]
  :java-source-paths ["src/java"]
  :main ^:skip-aot thrift-example.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
