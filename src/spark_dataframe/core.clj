(ns spark-dataframe.core
  (:refer-clojure :exclude [group-by])
  (:require [flambo.sql :as sql :refer [create-custom-schema group-by agg order-by window over]]
            [flambo.sql-functions :refer [col]]
            [flambo.api :as api]
            [clojure.reflect :refer [reflect]]
            [clojure.pprint :refer [print-table]]

            [spark-dataframe.p2p-report.generic :refer [decimalType]]
            [spark-dataframe.p2p-report.funding-circle :as fc]
            [spark-dataframe.p2p-report.core :as report-core]
            [spark-dataframe.p2p-report.generic :as generic]
            [spark-dataframe.sql :refer [as-col-array lit pivot sum create-dataframe]]
            )
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark SparkContext)
           (org.apache.spark.api.java JavaSparkContext)
           (org.apache.spark.sql Column)
           (org.apache.spark.sql functions RowFactory)
           (org.apache.spark.sql.types DataTypes)
    ;;(org.apache.spark.sql.functions$ MODULE$)
           (org.apache.spark.sql.expressions Window)
           (org.apache.spark SparkContext)))


(defn methods-info [obj]
  (print-table (sort-by :name (filter :exception-types (:members (reflect obj))))))

(def inputData "/home/mathieu/Dropbox/Finance/investment-transactions/fundingCircleMonthly")
(def outputData "/home/mathieu/Dropbox/Finance/investment-transactions/")

(def df0File (str outputData  "/Matlux_funding-circle_all_2017-01_2018-10.cvs"))

(defonce spark (.. (SparkSession/builder)
               (appName "Simple app")
               (master "local[*]")
               getOrCreate))

(defonce java-sc (new JavaSparkContext (.sparkContext spark
                                                      )))
(def  opts {"header" "true",
                     "date" "yyyy-MM-dd",
            "inferSchema" "true"})


(def df (.. spark
            read
            (schema fc/schema)
            (options opts)
            (csv df0File)))



(def df1 (report-core/clean-data :funding-circle df))

(comment


  ;; test dataframe
  (def test-df (create-dataframe spark
                                 [["Description", DataTypes/StringType, true]
                                  ["amount", DataTypes/LongType, true]]
                                  '( ( "interest payment 1" 2)
                                     ( "transfer" 40)
                                     ( "interest payment 2" 4)
                                    ( "fee" 1)
                                     ( "interest payment 3" 3))))

  (.show test-df 50 false)
  (.printSchema test-df)

  (.. test-df
      (withColumn "pence" (.multiply (col "amount") (lit 100)))
      (show 50 false))



  (.. test-df2
      (withColumn "ab" (.plus (col "col2") (col "+1")))
      (show 50 false))


  ;; show how to parse a dataframe
  (.show df 50 false)
  (.count df)



  (.. df
      (filter (.rlike (functions/col "Description") "Loan Part ID"))
      (show 50 false))


  (-> df
      (group-by (col "Description"))
      .count
      (.show 50 false)
      )
  (-> df
      (group-by (.rlike (col "Description") "Interest repayment for loan part (.+)"))
      .count
      (.show 50 false)
      )

  (.. df
      (withColumn "foobar"
                  (when
                    (.rlike (col "Description") "Loan Part ID")
                    (lit "LOANPART")))
      (show 50 false)
      )
  (.. df
      (withColumn "foobar"
                  (-> (when
                        (.rlike (col "Description") "Loan Part ID")
                        (lit "LOANPART"))
                      (.when (.rlike (col "Description") "EPDQ")
                        (lit "EPDQ"))))

      (show 50 false))

  (let [

        dfcat (->> fc/regexes
                   (map (fn [regex] (.. df
                                        (withColumn "foobar"
                                          (when (.rlike (col "Description") regex)
                                            (lit regex))))) )
                   (reduce (fn [acc df] (.union acc df))))]
    (.show dfcat 50 false)
    ;(.count dfcat)
    ;
    )

  (let [

        dfcat (->> fc/regexes
                   (map (fn [regex] (.. df
                                        (filter (.rlike (col "Description") regex)))) )
                   (reduce (fn [acc df] (.union acc df))))]
    (.count dfcat)
    ;(.show dfcat 50 false)
    )


  (def df1 (report-core/clean-data :funding-circle df))

  (.show df1 50 false)

  (def df2 (report-core/generate-report1 df1 "2018-04-01" "2018-12-01"))
  (.. (report-core/generate-report1 df1 "2017-01-01" "2018-12-01")
      (show 50 false))

  (let [[correct? missing]  (report-core/validate-cat :funding-circle df)]
    (.show missing)
    correct?)









  (def schema-test (sql/create-custom-schema
                     [["col1", DataTypes/StringType, true]
                      ["col2", DataTypes/StringType, true]]))

  (let [schema-test (sql/create-custom-schema
                      [["col1", DataTypes/StringType, true]
                       ["col2", DataTypes/StringType, true]])

        test-data (map into-array '(("bar1.1", "bar2.1") ("bar1.2", "bar2.2")))
        list-array-data (api/parallelize java-sc test-data)
        row-rdd (api/map list-array-data #(RowFactory/create %))
        test-df (.createDataFrame spark
                                  row-rdd schema-test)
        ]
    (.show test-df 50 false)
    ;test-df
    )

  ()
  (api/parallelize java-sc test-data)
  (-> (api/map into-array '(("bar1.1", "bar2.1") ("bar1.2", "bar2.2"))
           first type))

  (let [test-data ["{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":6,\"col2\":\"a\"}" "{\"col1\":5,\"col2\":\"b\"}"]
        test-df (sql/json-rdd spark (api/parallelize java-sc test-data))]
    (.show test-df 50 false))


  (def schema-test (sql/create-custom-schema
                [["col1", DataTypes/StringType, true]
                 ["col2", DataTypes/StringType, true]]))
  (def schema (sql/create-custom-schema
                [["col1", DataTypes/DateType, true]
                 ["col2", DataTypes/StringType, true]
                 ["col3", decimalType, true]
                 ]))
  (new JavaSparkContext (.sparkContext spark
                                       ))

  (let [test-data ["{\"col1\":4,\"col2\":\"a\"}" "{\"col1\":6,\"col2\":\"a\"}" "{\"col1\":5,\"col2\":\"b\"}"]
        test-df (sql/json-rdd spark (api/parallelize java-sc test-data))]
    (.show test-df 50 false))

  (methods-info spark)


  (def df (api/parallelize))

  (.show df)
  (.printSchema df)

  (.show df2)
  (.printSchema df2)

  (type df)



  (generic/generic-categories2col :GENERIC_TRANSFER_CATEGORY)
  (sql-sort df "year" "month")
  (sql/order-by df "year" "month")

  (let [[correct? missing]  (report-core/validate-cat :funding-circle df)]
    (.show missing)
    correct?)

  (def df1 (report-core/clean-data :funding-circle df))
  (def df2 (report-core/generate-report1 df1 "2018-04-01" "2018-12-01"))
  (.. (report-core/generate-report1 df1 "2017-01-01" "2018-12-01")
      (show 50 false))


  (.col pivoted-report (generic/generic-categories2col :GENERIC_TRANSFER_CATEGORY))
  final-report (.. pivoted-report
                   (withColumn "cum BT" (-> :GENERIC_TRANSFER_CATEGORY
                                            generic/generic-categories2col

                                            sqlf/col
                                            sum
                                            (sql/over w-spec)
                                            )))

  (let [w-spec (.rowsBetween (sql/order-by (sql/window) "year" "month") Long/MIN_VALUE 0)])
  (let [w-spec (.rowsBetween (Window/orderBy (into-array Column (map sqlf/col '("year" "month")))) Long/MIN_VALUE 0)])
  (into-array Column (map sqlf/col '("year" "month")))
  (.. (report-core/clean-data :funding-circle df)

      (show 50 false))

  (new org.apache.spark.sql.Column "date")
  (new Column "date")

  (def df3 (.. df
               (withColumn "month" (functions/month (new Column "date")))
               (withColumn "year" (functions/year (functions/col "date")))))


  (.. df3
      (show 500 false))
  dfrs0.filter (col ("Type") .rlike ("PartialSelloutRepayment")) .show (500, false)

  (.. df
      (filter (.rlike (functions/col "Description") "Loan Part ID"))
      ;(rlike "Loan Part ID")
      (show 50 false))
  (.. df
      (withColumn "foobar"
                  (functions/when
                    (.rlike (functions/col "Description") "Loan Part ID")
                    (functions/lit "LOANPART")))
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
                   (withColumn "FC type")))


      (def array [["test" DataTypes/StringType]])

      (defn create-custom-schema [array]
        (-> (map #(DataTypes/createStructField (first %) (second %) (nth % 2)) array)
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

      )
  )                                                         ;



(defn foo
  "I don't do a whole lot."
  [x]
  (println x "Hello, World!" spark))



(defn -main [& args]
  spark)