(ns spark-dataframe.core
  (:require [flambo.sql :as sql :refer [create-custom-schema]]
            [spark-dataframe.p2p-report.generic :refer [decimalType]]
            [spark-dataframe.p2p-report.funding-circle :as fc]
            [spark-dataframe.p2p-report.core :as report-core]
            )
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark.sql Column)
           (org.apache.spark.sql functions)
           (org.apache.spark.sql.types DataTypes)
    ;;(org.apache.spark.sql.functions$ MODULE$)
           ))

(def inputData "/home/mathieu/Dropbox/Finance/investment-transactions/fundingCircleMonthly")
(def outputData "/home/mathieu/Dropbox/Finance/investment-transactions/")

(def df0File (str outputData  "/Matlux_funding-circle_all_2017-01_2018-10.cvs"))

(defonce spark (.. (SparkSession/builder)
               (appName "Simple app")
               (master "local[*]")
               getOrCreate))
(def  opts {"header" "true",
                     "date" "yyyy-MM-dd",
            "inferSchema" "true"})


(def df (.. spark
            read
            (schema fc/schema)
            (options opts)
            (csv df0File)))

(def df2 (.. df
             (withColumn "month" (functions/month (new Column "date")))
             (withColumn "year" (functions/year (new Column "date")))))



(comment

  (.. (report-core/clean-data :funding-circle df)

      (show 50 false))

  (new org.apache.spark.sql.Column "date")
  (new Column "date")

  (def df3 (.. df
               (withColumn "month" (functions/month (new Column "date")))
               (withColumn "year" (functions/year (functions/col "date")))))


  (.. df3
      (show 500 false))
  dfrs0.filter(col("Type").rlike("PartialSelloutRepayment")).show(500, false)

  (.. df
      (filter (.rlike (functions/col "Description") "Loan Part ID"))
      ;(rlike "Loan Part ID")
      (show 50 false))
  (.. df
      (withColumn "foobar"
                  (functions/when
                    (.rlike (functions/col "Description") "Loan Part ID")
                    (functions/lit "LOANPART") ))
      (.. df
          (withColumn "foobar"
                      (.. (functions/when
                         (.rlike (functions/col "Description") "Loan Part ID")
                         (functions/lit "LOANPART"))
                          (when (.rlike (functions/col "Description") "EPDQ")
                            (functions/lit "EPDQ"))))

          (show 50 false))


  (for [genTypeInfoMap fc/types2generic-types
        genTypeInfo (._2 genTypeInfoMap)]
    [(._1 genTypeInfoMap) (:EXTRACT_REGEX (._2 genTypeInfo))])

  (def df2 (.. df
               (withColumn "FC type" )))


  (def array [["test" DataTypes/StringType]])

  (defn create-custom-schema [array]
    (-> (map #(DataTypes/createStructField (first %) (second %) (nth % 2))  array)
        DataTypes/createStructType))

  (create-custom-schema ["test" DataTypes/StringType])

  (sql/create-custom-schema
    [["id" DataTypes/IntegerType true]
     ["name" DataTypes/StringType true]
     ["seq" DataTypes/IntegerType true]])
  df

  ;;org.apache.spark.sql.Column$/MODULE$

  ;;(new org.apache.spark.sql.functions$/MODULE)
  (functions/month (new Column "date"))

  ;;(. month (. month org.apache.spark.sql.functions$/MODULE$ (new Column "date")))



  (.show df)
  (.printSchema df)
  )
  )


(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!" spark))



(defn -main [& args]
  spark)