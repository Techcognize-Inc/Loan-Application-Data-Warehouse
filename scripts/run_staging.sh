#!/usr/bin/env bash
set -euo pipefail

echo "Running staging build: staging.stg_loan_application_enriched"

docker exec -it de3-spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.executor.memory=2g \
  --conf spark.driver.memory=2g \
  --conf spark.sql.shuffle.partitions=16 \
  --jars /opt/spark/work-dir/jars/postgresql.jar \
  /opt/spark/work-dir/spark_jobs/build_staging.py

echo "Staging build finished. Running validations..."

docker exec -it de3-postgres psql -U postgres -d bankingdb -c \
'select count(*) as row_count from staging.stg_loan_application_enriched;'

docker exec -it de3-postgres psql -U postgres -d bankingdb -c \
'select count(*) - count(distinct "SK_ID_CURR") as dupes from staging.stg_loan_application_enriched;'

docker exec -it de3-postgres psql -U postgres -d bankingdb -c \
'select count(*) as null_keys from staging.stg_loan_application_enriched where "SK_ID_CURR" is null;'

docker exec -it de3-postgres psql -U postgres -d bankingdb -c \
'select "TARGET", count(*) from staging.stg_loan_application_enriched group by "TARGET" order by "TARGET";'

echo "✅ Validations done."