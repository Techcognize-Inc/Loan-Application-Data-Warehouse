{{ config(materialized='table') }}

-- just see what dbt actually sees for stg_applications
SELECT *
FROM {{ ref('stg_applications') }}
LIMIT 5

