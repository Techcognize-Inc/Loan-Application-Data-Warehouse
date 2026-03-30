#!/usr/bin/env bash
set -euo pipefail

echo "Landing warehouse tables from Postgres to Iceberg (Hadoop catalog)..."

docker exec -i de3-spark-master bash -lc "
  export HOME=/tmp
  mkdir -p /tmp/.ivy2/cache /tmp/.ivy2/jars

  cd /opt/spark/work-dir && \
  export PYTHONPATH=/opt/spark/work-dir && \

  # DB connection for container-to-container networking
  export DB_HOST=postgres && \
  export DB_PORT=5432 && \
  export DB_NAME=bankingdb && \
  export DB_USER=postgres && \
  export DB_PASSWORD=password && \

  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.driver.extraJavaOptions='-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2' \
    --conf spark.executor.extraJavaOptions='-Divy.cache.dir=/tmp/.ivy2/cache -Divy.home=/tmp/.ivy2' \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.iceberg.type=hadoop \
    --conf spark.sql.catalog.iceberg.warehouse=/opt/spark/work-dir/iceberg_warehouse \
    --conf spark.executor.memory=1g \
    --conf spark.driver.memory=1g \
    --conf spark.executor.cores=1 \
    --conf spark.cores.max=1 \
    --conf spark.sql.shuffle.partitions=2 \
    --conf spark.default.parallelism=2 \
    --jars /opt/spark/work-dir/jars/postgresql.jar \
    /opt/spark/work-dir/spark_jobs/land_warehouse_to_iceberg.py
"

echo "✅ Iceberg landing finished."