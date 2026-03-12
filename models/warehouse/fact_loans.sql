{{ config(materialized='table') }}

SELECT
    l."SK_ID_PREV"      AS loan_id,
    l."SK_ID_CURR"      AS customer_id,
    l."AMT_APPLICATION" AS application_amount,
    l."AMT_CREDIT"      AS credit_amount,
    l."AMT_ANNUITY"     AS annuity_amount,
    l."AMT_GOODS_PRICE" AS goods_price,
    l."NAME_CONTRACT_TYPE" AS contract_type,
    l."NAME_CONTRACT_STATUS" AS contract_status,
    l."CNT_PAYMENT"     AS payment_count,
    l."DAYS_DECISION"   AS decision_days
FROM {{ ref('stg_loans') }} l
