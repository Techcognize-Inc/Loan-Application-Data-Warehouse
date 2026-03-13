{{ config(materialized='view') }}

SELECT
    "SK_ID_BUREAU"      AS bureau_id,
    "SK_ID_CURR"        AS customer_id,
    "CREDIT_ACTIVE",
    "DAYS_CREDIT",
    "DAYS_CREDIT_ENDDATE",
    "AMT_CREDIT_SUM"
FROM raw.bureau