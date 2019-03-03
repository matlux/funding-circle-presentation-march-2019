(ns spark-dataframe.p2p-report.funding-circle
  (:require [flambo.sql :as sql :refer [create-custom-schema]]
            [spark-dataframe.p2p-report.generic :as generic :refer [decimalType]]
            )
  (:import (org.apache.spark.sql SparkSession)
           (org.apache.spark.sql Column)
           (org.apache.spark.sql functions)
           (org.apache.spark.sql.types DataTypes)
           ))


;; Funding Circle Regexes
(def fc-regexes {:LOAN_PART_REGEX_EXTRACT                 "Loan Part ID (.+) : Principal (.+), Interest (.+), Delta (.+), Fee (.+)"

                :LOAN_OFFER_REGEX_EXTRACT                "Loan offer on (.+) - (.+)"
                :TRANSFERIN_REGEX_EXTRACT                "EPDQ ID: (.+) - TRANSFERIN ORDERID: (.+)"
                :WITHDRAWAL_REGEX_EXTRACT                "FC Len Withdrawal"

                :SERVICING_FEE_REGEX_EXTRACT             "Servicing fee for Loan ID N/A; Loan Part ID (.+); Investor ID (.+)"
                :SERVICING_FEE_REGEX_EXTRACT2            "Servicing fee for loan part (.+)"

                :PRINCIPAL_REPAYMENT_REGEX_EXTRACT       "Principal repayment for loan part (.+)"
                :EARLY_PRINCIPAL_REPAYMENT_REGEX_EXTRACT "Early principal repayment for loan part (.+)"

                :PRINCIPAL_RECOVERY_REGEX_EXTRACT        "Principal recovery repayment for loan part (.+)"

                :INTEREST_REPAYMENT_REGEX_EXTRACT        "Interest repayment for loan part (.+)"
                :EARLY_INTEREST_REPAYMENT_REGEX_EXTRACT  "Early interest repayment for loan part (.+)"
                })



;; Funding Circle Types or categories of transactions

(def fc-types {;;:CANCELLATION_TYPE nil
               :FC_LOAN_PART_TYPE                 "Loan Part"

               :FC_LOAN_OFFER_TYPE                "Loan offer"

               :FC_TRANSFERIN_TYPE                "TRANSFER IN"
               ;; :TRANSFERIN_CARD_TYPE nil
               :FC_WITHDRAWAL_TYPE                "Withdrawal"
               :FC_FEE_TYPE                       "Servicing fee"
               :FC_FINAL_FEE_TYPE2                "Servicing fee final"

               ;; REPAYMENT
               :FC_PRINCIPAL_REPAYMENT_TYPE       "Principal repayment"
               :FC_EARLY_PRINCIPAL_REPAYMENT_TYPE "Early principal repayment"
               ;; :SELLOUT_TYPE nil
               ;; :PARTIAL_SELLOUT_TYPE = nil

               :FC_PRINCIPAL_RECOVERY_TYPE        "Principal recovery"

               :FC_INTEREST_REPAYMENT_TYPE        "Interest repayment"
               :FC_EARLY_INTEREST_REPAYMENT_TYPE  "Early interest repayment"
               })

(def types2generic-types
  {
   ;; LOAN
   ;; :CANCELLATION_TYPE Map {GENERIC_CANCELLATION_TYPE {}},
   :FC_LOAN_PART_TYPE                 {:GENERIC_LOAN_PART_TYPE {:EXTRACT_REGEX :LOAN_PART_REGEX_EXTRACT}},

   ;; MISC
   :FC_LOAN_OFFER_TYPE                {:GENERIC_LOAN_OFFER_TYPE {:EXTRACT_REGEX :LOAN_OFFER_REGEX_EXTRACT}},

   ;; TRANSFER
   :FC_TRANSFERIN_TYPE                {:GENERIC_TRANSFERIN_TYPE {:EXTRACT_REGEX :TRANSFERIN_REGEX_EXTRACT}},
   ;; :TRANSFERIN_CARD_TYPE {GENERIC_TRANSFERIN_CARD_TYPE {}},
   :FC_WITHDRAWAL_TYPE                {:GENERIC_WITHDRAWAL_TYPE {:EXTRACT_REGEX :WITHDRAWAL_REGEX_EXTRACT}},

   ;; FEE
   :FC_FEE_TYPE                       {:GENERIC_FEE_TYPE {:EXTRACT_REGEX :SERVICING_FEE_REGEX_EXTRACT}},
   :FC_FINAL_FEE_TYPE2                {:GENERIC_FINAL_FEE_TYPE2 {:EXTRACT_REGEX :SERVICING_FEE_REGEX_EXTRACT2}},

   ;; REPAYMENT
   :FC_PRINCIPAL_REPAYMENT_TYPE       {:GENERIC_PRINCIPAL_REPAYMENT_TYPE {:EXTRACT_REGEX :PRINCIPAL_REPAYMENT_REGEX_EXTRACT}},
   :FC_EARLY_PRINCIPAL_REPAYMENT_TYPE {:GENERIC_EARLY_PRINCIPAL_REPAYMENT_TYPE {:EXTRACT_REGEX :EARLY_PRINCIPAL_REPAYMENT_REGEX_EXTRACT}},
   ;; :SELLOUT_TYPE {GENERIC_SELLOUT_TYPE {}},

   ;; RECOVERY
   ;; :PARTIAL_SELLOUT_TYPE {GENERIC_PARTIAL_SELLOUT_TYPE {}},
   :FC_PRINCIPAL_RECOVERY_TYPE        {:GENERIC_PRINCIPAL_RECOVERY_TYPE {:EXTRACT_REGEX :PRINCIPAL_RECOVERY_REGEX_EXTRACT}},

   ;; INTEREST
   :FC_INTEREST_REPAYMENT_TYPE        {:GENERIC_INTEREST_REPAYMENT_TYPE {:EXTRACT_REGEX :INTEREST_REPAYMENT_REGEX_EXTRACT}},
   :FC_EARLY_INTEREST_REPAYMENT_TYPE  {:GENERIC_EARLY_INTEREST_REPAYMENT_TYPE {:EXTRACT_REGEX :EARLY_INTEREST_REPAYMENT_REGEX_EXTRACT}}
   })


(def types (keys fc-types))


(comment



  )


(def type2regex (into {} (for [[genTypeKey genTypeVal] types2generic-types
                               [_ regex] genTypeVal]
                           [genTypeKey (fc-regexes (:EXTRACT_REGEX regex))])))

(def type2generic-cat (into {} (for [[genTypeKey genTypeVal] types2generic-types
                                     [type _] genTypeVal]
                                 [genTypeKey (generic/generic-type2category type)
                                  ])))
(def type2generic-type (into {} (for [[genTypeKey genTypeVal] types2generic-types
                                     [type _] genTypeVal]
                                 [genTypeKey type
                                  ])))

(def regexes (map type2regex (keys type2generic-type)))

(def schema (sql/create-custom-schema
                 [["Date", DataTypes/DateType, true]
                  ["Description", DataTypes/StringType, true]
                  ["Paid In", decimalType, true]
                  ["Paid Out", decimalType, true]
                  ]))

