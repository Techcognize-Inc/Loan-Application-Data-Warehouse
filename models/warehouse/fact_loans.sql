{{ config(materialized='table') }}

SELECT
    a.customer_id               AS customer_key,
    AVG(a.loan_amount)          AS avg_credit_amount,
    AVG(a.income)               AS avg_income,
    COUNT(a.customer_id)        AS loan_application_count,
    b.bureau_id                 AS bureau_id,
    SUM(b."AMT_CREDIT_SUM")     AS total_bureau_credit
FROM {{ ref('stg_applications') }} a
LEFT JOIN {{ ref('stg_bureau') }} b
    ON a.customer_id = b.customer_id
GROUP BY a.customer_id, b.bureau_id
