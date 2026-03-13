{{ config(materialized='table') }}

SELECT
    "SK_ID_BUREAU"           AS bureau_id,
    "SK_ID_CURR"             AS customer_id,
    "CREDIT_ACTIVE"          AS credit_active,
    "CREDIT_CURRENCY"        AS credit_currency,
    "DAYS_CREDIT"            AS days_credit,
    "CREDIT_DAY_OVERDUE"     AS credit_day_overdue,
    "DAYS_CREDIT_ENDDATE"    AS days_credit_enddate,
    "DAYS_ENDDATE_FACT"      AS days_enddate_fact,
    "AMT_CREDIT_MAX_OVERDUE" AS amt_credit_max_overdue,
    "CNT_CREDIT_PROLONG"     AS cnt_credit_prolong,
    "AMT_CREDIT_SUM"         AS amt_credit_sum,
    "AMT_CREDIT_SUM_DEBT"    AS amt_credit_sum_debt,
    "AMT_CREDIT_SUM_LIMIT"   AS amt_credit_sum_limit,
    "AMT_CREDIT_SUM_OVERDUE" AS amt_credit_sum_overdue,
    "CREDIT_TYPE"            AS credit_type,
    "DAYS_CREDIT_UPDATE"     AS days_credit_update,
    "AMT_ANNUITY"            AS amt_annuity
FROM raw.bureau