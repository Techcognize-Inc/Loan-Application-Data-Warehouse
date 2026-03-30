#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 40); do
  if docker exec -i de3-postgres psql -U postgres -d bankingdb -c "select 1;" >/dev/null 2>&1; then
    echo "✅ PostgreSQL is ready"
    break
  fi
  echo "  Attempt $i/40 — retrying in 3s..."
  sleep 3
done

echo "Running DQ validation: raw.* → silver.* + quarantine.*"

# Ensure schemas exist
docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
  "CREATE SCHEMA IF NOT EXISTS silver;
   CREATE SCHEMA IF NOT EXISTS quarantine;"

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
    --conf spark.executor.memory=2g \
    --conf spark.driver.memory=2g \
    --conf spark.executor.cores=1 \
    --conf spark.cores.max=1 \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.default.parallelism=2 \
    --conf spark.network.timeout=1200s \
    --conf spark.executor.heartbeatInterval=60s \
    --conf spark.worker.timeout=1200 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.executorEnv.PYTHONPATH=/opt/spark/work-dir \
    --jars /opt/spark/work-dir/jars/postgresql.jar \
    /opt/spark/work-dir/spark_jobs/dq_validate.py
"

echo "✅ DQ validation finished — clean data in silver.*, rejects in quarantine.*"