{{ config(materialized='view') }}

SELECT
    "SK_ID_CURR"        AS customer_id,
    "AMT_CREDIT"        AS loan_amount,
    "AMT_INCOME_TOTAL"  AS income,
    "AMT_ANNUITY"       AS annuity,
    "TARGET"            AS default_flag,
    "CODE_GENDER"       AS gender,
    "NAME_EDUCATION_TYPE" AS education_type,
    "OCCUPATION_TYPE",
    "ORGANIZATION_TYPE"
FROM raw.application_train