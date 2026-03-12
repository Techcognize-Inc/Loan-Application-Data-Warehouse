{{ config(materialized='table') }}

SELECT DISTINCT
    "SK_ID_BUREAU"          AS bureau_id,
    "SK_ID_CURR"            AS customer_id,
    "CREDIT_ACTIVE"         AS credit_status,
    "CREDIT_TYPE"           AS credit_type,
    "CREDIT_CURRENCY"       AS currency,
    "DAYS_CREDIT"           AS days_credit,
    "DAYS_CREDIT_ENDDATE"   AS credit_end_date,
    "AMT_CREDIT_SUM"        AS credit_amount,
    "AMT_CREDIT_SUM_DEBT"   AS credit_debt,
    "AMT_CREDIT_SUM_OVERDUE" AS overdue_amount
FROM {{ ref('stg_applications') }}

