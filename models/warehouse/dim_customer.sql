{{ config(materialized='table') }}

SELECT DISTINCT
    customer_id             AS customer_key,
    default_flag            AS target,
    gender                  AS code_gender,
    income                  AS amt_income_total,
    loan_amount             AS amt_credit
FROM {{ ref('stg_applications') }}
WHERE customer_id IS NOT NULL
