SELECT
    SK_ID_CURR AS customer_id,
    CODE_GENDER,
    FLAG_OWN_CAR,
    FLAG_OWN_REALTY,
    CNT_CHILDREN,
    AMT_INCOME_TOTAL
FROM staging.stg_loan_application_enriched
