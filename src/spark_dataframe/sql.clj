(ns spark-dataframe.sql
  (:require [flambo.sql-functions :as sqlf]
            [flambo.sql :as sql]
            [flambo.api :as api])
  (:import (org.apache.spark.sql Column)
           (org.apache.spark.sql functions RowFactory)
           (org.apache.spark.api.java JavaSparkContext))
  )


(defn as-col-array
  [exprs]
  (into-array Column (map sqlf/col exprs)))

(defn lit [x] (functions/lit x))

(defn pivot
  "pivot grouped data using the specified expressions"
  [df expr xs]
  (.pivot df (sqlf/col expr) xs))



(defn sum [x]
  (functions/sum x))

(defn create-dataframe [spark  schema data ]
  (let [java-sc (new JavaSparkContext (.sparkContext spark
                                                     ))
        schema-test (sql/create-custom-schema
                      schema)

              test-data (map #(into-array Object %) data)
              list-array-data (api/parallelize java-sc test-data)
              row-rdd (api/map list-array-data #(RowFactory/create %))
              test-df (.createDataFrame spark
                                        row-rdd schema-test)
              ]
    ; (.show test-df 50 false)
          test-df
          ))