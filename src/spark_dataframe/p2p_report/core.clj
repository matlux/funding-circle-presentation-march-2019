(ns spark-dataframe.p2p-report.core
  (:require
    [spark-dataframe.p2p-report.generic :as generic]
    [spark-dataframe.p2p-report.funding-circle :as fc]
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







                                                                ;def getFillInTypeFct
  ;(providerType : Providers.Provider) :
  ;(String => String) => Column =
  ;{
  ; providerType match {
  ;                     case Providers.FC => fillinType (FcTypes, FcTypes2Regex, "Description")
  ;                          case Providers.RATESETTER => fillinType (RsTypes, RsTypes2Regex, "RsType")
  ;                     }
  ; }
  ;
  ;
  ;def providerType
  ;(providerType  Providers.Provider)  Column = {
  ;                                                val f (String => String) => Column = getFillInTypeFct (providerType)
  ;                                                f   (identity)
  ;                                                }
  ;def genType
  ;(providerType  Providers.Provider)  Column = {
  ;                                                val f  (String => String) => Column = getFillInTypeFct (providerType)
  ;                                                    f (genType => provider2genType (providerType, genType))
  ;                                                }
  ;def genCat
  ;(providerType  Providers.Provider)  Column = {
  ;                                                getFillInTypeFct (providerType) .apply (genType => provider2genCats (providerType, genType))
  ;                                                }

  )