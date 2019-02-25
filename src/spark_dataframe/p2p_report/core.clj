(ns spark-dataframe.p2p-report.core
  (:require [spark-dataframe.p2p-report.funding-circle :as fc]
            [clojure.core.match :refer [match]])
  (:import
           (org.apache.spark.sql Column)
           (org.apache.spark.sql functions)
           ))


(def provider-functions
  {:funding-cicle {fc/types2generic-types,fc/type2generic-cat, fc/type2regex, "Description"}
   :rate-setter {}})


(defn fillinType [providerTypes providerType2Regex c f]
  (reduce  (fn [acc t] (.. acc
                           (when (.rlike (functions/col c) (providerType2Regex t))
                             (functions/lit (f t)))))
           (functions/when
             (.rlike (functions/col c) (providerType2Regex (first providerTypes)))
             (functions/lit (f (first providerTypes)))) (next providerTypes)))

(comment



  (.. df
      (withColumn "foobar"
                  (.. (functions/when
                        (.rlike (functions/col "Description") "Loan Part ID")
                        (functions/lit "LOANPART"))
                      (when (.rlike (functions/col "Description") "EPDQ")
                        (functions/lit "EPDQ"))))

      (show 50 false))

  (match :foobar
         :foobar 42)


  (defn getFillInTypeFct [provider]
    (match provider
           :funding-cicle
           :rate-setter))


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