{{ config(materialized='table') }}

SELECT DISTINCT
    "SK_ID_CURR"            AS customer_id,
    "CODE_GENDER"           AS gender,
    "NAME_FAMILY_STATUS"    AS family_status,
    "NAME_INCOME_TYPE"      AS income_type,
    "AMT_INCOME_TOTAL"      AS income,
    "CNT_CHILDREN"          AS children_count,
    "CNT_FAM_MEMBERS"       AS family_members,
    "NAME_EDUCATION_TYPE"   AS education,
    "NAME_HOUSING_TYPE"     AS housing_type,
    "OCCUPATION_TYPE"       AS occupation
FROM {{ ref('stg_bureau') }}
