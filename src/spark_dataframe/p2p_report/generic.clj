(ns spark-dataframe.p2p-report.generic
  (:import (org.apache.spark.sql.types DataTypes)))


;;comment

(def generic-categories2col
  {

   :GENERIC_LOAN_CATEGORY "LOAN cat"
   :GENERIC_MISC_CATEGORY "MISC cat"
   :GENERIC_TRANSFER_CATEGORY "TRANSFER cat"
   :GENERIC_FEE_CATEGORY "FEE cat"
   :GENERIC_REPAYMENT_CATEGORY "REPAYMENT cat"
   :GENERIC_PRINCIPAL_RECOVERY_CATEGORY "RECOVERY cat"
   :GENERIC_INTEREST_CATEGORY "INTEREST cat"

   })

(def generic-types2col
  {

                                                          ;; LOAN
   :GENERIC_CANCELLATION_TYPE "Cancellation of order gen type"  ;opposite or "Lend order"
   :GENERIC_LOAN_PART_TYPE "LOAN_PART gen type"               ;order ready to be matched. opposite or "Cancellation of order"

    ;MISC
   :GENERIC_LOAN_OFFER_TYPE "LOAN_OFFER gen type"

    ;TRANSFER
   :GENERIC_TRANSFERIN_TYPE "Bank transfer gen type"
   :GENERIC_TRANSFERIN_CARD_TYPE "Card payment processed gen type"
   :GENERIC_WITHDRAWAL_TYPE "Next Day Money Withdrawal request gen type"

    ;FEE
   :GENERIC_FEE_TYPE "FEE gen type"
   :GENERIC_FINAL_FEE_TYPE2 "FINAL_FEE gen type"

    ;REPAYMENT
   :GENERIC_PRINCIPAL_REPAYMENT_TYPE "Monthly PRINCIPAL_REPAYMENT gen type"
   :GENERIC_EARLY_PRINCIPAL_REPAYMENT_TYPE "EARLY_PRINCIPAL_REPAYMENT gen type"
   :GENERIC_SELLOUT_TYPE "RepaymentSellOut gen type"
   :GENERIC_PARTIAL_SELLOUT_TYPE "PartialSelloutRepayment gen type"

    ;RECOVERY
   :GENERIC_PRINCIPAL_RECOVERY_TYPE "PRINCIPAL_RECOVERY gen type"

    ;INTEREST
   :GENERIC_INTEREST_REPAYMENT_TYPE "Interest"
   :GENERIC_EARLY_INTEREST_REPAYMENT_TYPE= "EARLY_INTEREST_REPAYMENT gen type"
   :GENERIC_INTEREST_SELLOUT_TYPE "INTEREST_SELLOUT gen type"


   })

(def generic-categories
  {
   :GENERIC_LOAN_CATEGORY               {
                                         :GENERIC_CANCELLATION_TYPE {},
                                         :GENERIC_LOAN_PART_TYPE    {}
                                         },
   :GENERIC_MISC_CATEGORY               {
                                         :GENERIC_LOAN_OFFER_TYPE {}
                                         },
   :GENERIC_TRANSFER_CATEGORY           {
                                         :GENERIC_TRANSFERIN_TYPE      {},
                                         :GENERIC_TRANSFERIN_CARD_TYPE {},
                                         :GENERIC_WITHDRAWAL_TYPE      {}
                                         },
   :GENERIC_FEE_CATEGORY                {
                                         :GENERIC_FEE_TYPE        {},
                                         :GENERIC_FINAL_FEE_TYPE2 {}
                                         },
   :GENERIC_REPAYMENT_CATEGORY          {
                                         :GENERIC_PRINCIPAL_REPAYMENT_TYPE       {},
                                         :GENERIC_EARLY_PRINCIPAL_REPAYMENT_TYPE {},
                                         :GENERIC_SELLOUT_TYPE                   {},
                                         :GENERIC_PARTIAL_SELLOUT_TYPE           {}
                                         },
   :GENERIC_PRINCIPAL_RECOVERY_CATEGORY {
                                         :GENERIC_PRINCIPAL_RECOVERY_TYPE {}
                                         },

   :GENERIC_INTEREST_CATEGORY           {
                                         :GENERIC_INTEREST_REPAYMENT_TYPE       {},
                                         :GENERIC_EARLY_INTEREST_REPAYMENT_TYPE {},
                                         :GENERIC_INTEREST_SELLOUT_TYPE         {}
                                         }
   })

(def generic-type2category
  (into {}  (for [
         [catk catv] generic-categories
         [atype _] catv
         ]
     [atype catk])))


(def decimalType (DataTypes/createDecimalType 15 2))