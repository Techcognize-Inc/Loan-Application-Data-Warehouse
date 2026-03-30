{{ config(materialized='table') }}

with bureau_agg as (

    select
        b.customer_id,
        count(distinct b.bureau_id) as bureau_record_count,
        sum(b."AMT_CREDIT_SUM") as total_bureau_credit
    from {{ ref('stg_bureau') }} b
    group by b.customer_id

),

application_agg as (

    select
        a.customer_id as customer_key,
        avg(a.loan_amount) as avg_credit_amount,
        avg(a.income) as avg_income,
        count(*) as loan_application_count
    from {{ ref('stg_applications') }} a
    group by a.customer_id

)

select
    a.customer_key,
    a.avg_credit_amount,
    a.avg_income,
    a.loan_application_count,
    coalesce(b.bureau_record_count, 0) as bureau_record_count,
    coalesce(b.total_bureau_credit, 0) as total_bureau_credit
from application_agg a
left join bureau_agg b
    on a.customer_key = b.customer_id
