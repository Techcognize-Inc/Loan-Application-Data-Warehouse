#!/usr/bin/env bash
set -euo pipefail

echo "Running staging validations..."

echo "1) Confirm staging table exists"
docker exec -i de3-postgres psql -U postgres -d bankingdb -c "\dt staging.*"

echo "2) Check row count"
docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
'select count(*) as row_count from staging.stg_loan_application_enriched;'

echo "3) Check duplicate keys (must be 0)"
DUPES=$(docker exec -i de3-postgres psql -U postgres -d bankingdb -t -A -c \
'select count(*) - count(distinct "SK_ID_CURR") as dupes
 from staging.stg_loan_application_enriched;')

echo "Duplicate keys: $DUPES"
if [ "$DUPES" != "0" ]; then
  echo "❌ Validation failed: duplicate SK_ID_CURR values found"
  exit 1
fi

echo "4) Check null keys (must be 0)"
NULL_KEYS=$(docker exec -i de3-postgres psql -U postgres -d bankingdb -t -A -c \
'select count(*) as null_keys
 from staging.stg_loan_application_enriched
 where "SK_ID_CURR" is null;')

echo "Null keys: $NULL_KEYS"
if [ "$NULL_KEYS" != "0" ]; then
  echo "❌ Validation failed: null SK_ID_CURR values found"
  exit 1
fi

echo "5) Check TARGET distribution"
docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
'select "TARGET", count(*)
 from staging.stg_loan_application_enriched
 group by "TARGET"
 order by "TARGET";'

echo "6) Show table structure"
docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
'\d staging.stg_loan_application_enriched'

echo "7) Show sample important columns"
docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
'select "SK_ID_CURR", "TARGET", "AMT_CREDIT", bureau_records_cnt, prev_app_cnt
 from staging.stg_loan_application_enriched
 limit 20;'

echo "✅ Staging validations passed."