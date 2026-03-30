#!/usr/bin/env bash
set -euo pipefail

echo "Waiting for PostgreSQL to be ready..."
for i in $(seq 1 20); do
  if docker exec de3-postgres pg_isready -U postgres -d bankingdb; then
    echo "✅ PostgreSQL is ready"
    break
  fi
  echo "  Attempt $i/20 — retrying in 3s..."
  sleep 3
done

echo "Running raw ingestion: raw.* tables"
docker exec -i de3-postgres psql -U postgres -d bankingdb -c \
"CREATE SCHEMA IF NOT EXISTS raw; CREATE SCHEMA IF NOT EXISTS staging; CREATE SCHEMA IF NOT EXISTS warehouse; CREATE SCHEMA IF NOT EXISTS quarantine; CREATE SCHEMA IF NOT EXISTS silver;"

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
    /opt/spark/work-dir/spark_jobs/ingest_raw.py
"

echo "✅ Raw ingestion finished."