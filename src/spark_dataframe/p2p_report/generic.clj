(ns spark-dataframe.p2p-report.generic
  (:import (org.apache.spark.sql.types DataTypes)))


;;comment


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
  (for [
        [catk catv] generic-categories
        [atype _] catv
        ]
    [atype catk]))


(def decimalType (DataTypes/createDecimalType 15 2))