#!/usr/bin/env bash
set -euo pipefail

echo "Running staging build: staging.stg_loan_application_enriched"

docker exec -i de3-spark-master bash -c "
  cd /opt/spark/work-dir && \
  export PYTHONPATH=/opt/spark/work-dir && \
  export DB_HOST=postgres && \
  export DB_PORT=5432 && \
  export DB_NAME=bankingdb && \
  export DB_USER=postgres && \
  export DB_PASSWORD=password && \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --conf spark.executor.memory=1g \
    --conf spark.driver.memory=1g \
    --conf spark.executor.cores=1 \
    --conf spark.cores.max=1 \
    --conf spark.sql.shuffle.partitions=4 \
    --conf spark.default.parallelism=4 \
    --conf spark.network.timeout=600s \
    --conf spark.executor.heartbeatInterval=60s \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
    --jars /opt/spark/work-dir/jars/postgresql.jar \
    /opt/spark/work-dir/spark_jobs/build_staging.py
"

echo "Staging build finished. Running validations..."

docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
'select count(*) as row_count from staging.stg_loan_application_enriched;'

docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
'select count(*) - count(distinct "SK_ID_CURR") as dupes from staging.stg_loan_application_enriched;'

docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
'select count(*) as null_keys from staging.stg_loan_application_enriched where "SK_ID_CURR" is null;'

docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
'select "TARGET", count(*) from staging.stg_loan_application_enriched group by "TARGET" order by "TARGET";'

echo "✅ Validations done."