(defproject spark-dataframe "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.9.0"]
                 [yieldbot/flambo "0.8.2"]
                 [org.clojure/core.match "0.3.0-alpha5"]
                 
                 [org.apache.spark/spark-core_2.11 "2.2.0"]
                 [org.apache.spark/spark-sql_2.11 "2.2.0"]
                 [org.apache.spark/spark-hive_2.11 "2.2.0"]
                 ]
  :profiles {:dev
             {:aot [spark-dataframe.core]}}
  
  )
