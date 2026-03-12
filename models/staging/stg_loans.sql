{{ config(materialized='view') }}

SELECT
    customer_id,
    loan_amount,
    income,
    annuity,
    default_flag
FROM {{ ref('stg_applications') }}