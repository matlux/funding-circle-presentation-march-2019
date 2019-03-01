(ns spark-dataframe.p2p-report.core
  (:refer-clojure :exclude [group-by])
  (:require
    [flambo.sql :as sql :refer [create-custom-schema group-by agg order-by window over select]]
    [flambo.sql-functions :refer [col]]
    [spark-dataframe.p2p-report.generic :as generic]
    [spark-dataframe.p2p-report.funding-circle :as fc]
    [spark-dataframe.sql :refer [as-col-array lit pivot sum]]
    [clojure.core.match :refer [match]])
  (:import
           (org.apache.spark.sql Column)
           (org.apache.spark.sql functions)
           ))


(def provider-functions
  {:funding-circle {:types2generic-types fc/type2generic-type
                    :type2generic-cat fc/type2generic-cat
                    :type2regex fc/type2regex
                    :type2col fc/fc-types
                   :column "Description"}
   :rate-setter {}})


(defn fillinType [prov-types providerType2Regex type2col c f]
  (reduce  (fn [acc t] (.. acc
                           (when (.rlike (functions/col c) (providerType2Regex t))
                             (functions/lit (f t)))))
           (functions/when
             (.rlike (functions/col c) (providerType2Regex (first prov-types)))
             (functions/lit (f (first prov-types)))) (next prov-types)))

(comment
  (def prov-types fc/types)
  (def providerType2Regex fc/type2regex)

  (def c "Description")
  (def f identity)
  (def t (first fc/types)))


(defn getFillInTypeFct [provider]
  (match provider
         :funding-circle (partial fillinType fc/types, fc/type2regex fc/fc-types "Description")
         :rate-setter))

(defn providerType [provider]
  ((getFillInTypeFct provider) (get-in provider-functions [provider :type2col])))

(defn genType [provider]
  ((getFillInTypeFct provider)
    (fn [genType]
      (generic/generic-types2col
        ((get-in provider-functions [:funding-circle :types2generic-types]) genType)))))

(defn genCat [provider]
  ((getFillInTypeFct provider)
    (fn [genType]
      (generic/generic-categories2col
        ((get-in provider-functions [:funding-circle :type2generic-cat]) genType)))))


(defn clean-data [provider df]
  (match provider
         :funding-circle (.. df
                             (withColumn "FC type" (providerType provider))
                             (withColumn "type" (genType provider))
                             (withColumn "cat" (genCat provider))
                             (withColumn "Amount"
                                         (.. (functions/when (.isNotNull (functions/col "Paid Out"))
                                                             (.minus (functions/lit 0) (functions/col "Paid Out")))
                                             (when (.isNotNull (functions/col "Paid In"))
                                               (functions/col "Paid In"))))
                             (withColumn "month" (functions/month (new Column "date")))
                             (withColumn "year" (functions/year (new Column "date"))))))

(defn cat2col [gen-cat]
  (-> gen-cat generic/generic-categories2col col))

(defn generate-report1 [df start-date end-date]
  (let [ ;start-date "2018-04-01"
        ;end-date "2018-12-01"
        w-spec (-> (window)
                   (order-by "year" "month")
                   (.rowsBetween Long/MIN_VALUE 0))
        pivoted-report (-> df
                           (.filter (.gt (col "date") (lit start-date)))
                           (.filter (.lt (col "date") (lit end-date)))
                           (group-by "year" "month")
                           (.pivot "cat"
                                   (map generic/generic-categories2col
                                        '(:GENERIC_TRANSFER_CATEGORY :GENERIC_INTEREST_CATEGORY :GENERIC_FEE_CATEGORY :GENERIC_PRINCIPAL_RECOVERY_CATEGORY)))
                           (agg (sum "amount"))
                           (order-by "year" "month"))
        final-report (-> pivoted-report
                         (.withColumn "cum BT" (over (sum (cat2col :GENERIC_INTEREST_CATEGORY)) w-spec))
                         (.withColumn "cum interest" (over (sum (cat2col :GENERIC_INTEREST_CATEGORY)) w-spec))
                         (.withColumn "cum fee" (over (sum (cat2col :GENERIC_INTEREST_CATEGORY)) w-spec))
                         (.withColumn "cum recovery" (over (sum (cat2col :GENERIC_INTEREST_CATEGORY)) w-spec))
                         (.withColumn "cum return" (.plus (col "cum interest") (col "cum fee")))
                         (select "year","month" (cat2col :GENERIC_TRANSFER_CATEGORY) "cum BT" (cat2col :GENERIC_INTEREST_CATEGORY)
                                 "cum interest" (cat2col :GENERIC_FEE_CATEGORY) "cum fee" (cat2col :GENERIC_PRINCIPAL_RECOVERY_CATEGORY)
                                 "cum recovery" "cum return")
                         )
        ]
    ;(.. final-report (show 50 false))
    ;pivoted-report
    final-report
    ))

(comment

  ((getFillInTypeFct provider)
    (fn [genType]
      (generic/generic-categories2col
        ((get-in provider-functions [:funding-circle :type2generic-cat]) :FC_INTEREST_REPAYMENT_TYPE))))

  ((getFillInTypeFct provider)
    (fn [genType]
      (generic/generic-types2col
        ((get-in provider-functions
                 [:funding-circle :types2generic-types])
          :FC_INTEREST_REPAYMENT_TYPE))))

  (providerType :funding-circle)
  (genType :funding-circle)
  (genCat :funding-circle)

(def provider :funding-circle)

  (.. df
      (withColumn "foobar"
                  (.. (functions/when
                        (.rlike (functions/col "Description") "Loan Part ID")
                        (functions/lit "LOANPART"))
                      (when (.rlike (functions/col "Description") "EPDQ")
                        (functions/lit "EPDQ"))))

      (show 50 false))


  )