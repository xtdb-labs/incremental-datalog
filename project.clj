(defproject incremental-datalog "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.10.1"]
                 [io.lambdaforge/datalog-parser "0.1.1"]
                 [org.clojure/math.combinatorics "0.1.6"]
                 [org.clojure/core.async "0.4.500"]
                 [while-let "0.2.0"]
                 [net.cgrand/xforms "0.19.2"]
                 [io.replikativ/datahike "0.2.0"]]
  :main ^:skip-aot incremental-datalog.core
  :target-path "target/%s"
  :profiles {:uberjar {:aot :all}})
