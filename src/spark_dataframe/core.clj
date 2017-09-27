(ns spark-dataframe.core
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark.sql Column)
           (org.apache.spark.sql functions)
    ;;(org.apache.spark.sql.functions$ MODULE$)
           ))



(def spark (.. (SparkSession/builder)
               (appName "Simple app")
               (master "local[*]")
               getOrCreate))
(def  opts {"header" "true",
                     "timestampFormat" "dd/MM/yyyy",
            "inferSchema" "true"})

(def df (.. spark
            read
            (options opts)
            (csv "/Users/mathieu/Dropbox/finance/investment-transactions/Matlux_rate-setter_LenderTransactions_all_2017-07-31.csv")))

(def df2 (.. df
             (withColumn "month" (functions/month (new Column "date")))
             (withColumn "year" (functions/year (new Column "date")))))


(comment
  (new org.apache.spark.sql.Column "date")
  (new Column "date")



  df

  ;;org.apache.spark.sql.Column$/MODULE$

  ;;(new org.apache.spark.sql.functions$/MODULE)
  (functions/month (new Column "date"))

  ;;(. month (. month org.apache.spark.sql.functions$/MODULE$ (new Column "date")))



  (.show df2)
  )


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!" spark))



(defn -main [& args]
  spark)