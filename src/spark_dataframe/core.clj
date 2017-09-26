(ns spark-dataframe.core
  (:import (org.apache.spark.sql SparkSession)))



(def spark (.. (SparkSession/builder)
               (appName "Simple app")
               (master "local[*]")
               getOrCreate))

(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!" spark))


