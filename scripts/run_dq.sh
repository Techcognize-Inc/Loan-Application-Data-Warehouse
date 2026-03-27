#!/usr/bin/env bash
set -euo pipefail

echo "Running DQ validation: raw.* → silver.* + quarantine.*"

# Ensure silver schema exists (quarantine already created in run_ingest.sh)
docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
  "CREATE SCHEMA IF NOT EXISTS silver;"

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
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.default.parallelism=2 \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
    --jars /opt/spark/work-dir/jars/postgresql.jar \
    /opt/spark/work-dir/spark_jobs/dq_validate.py
"

echo "✅ DQ validation finished — clean data in silver.*, rejects in quarantine.*"