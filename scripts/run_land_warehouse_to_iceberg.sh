#!/usr/bin/env bash
set -euo pipefail

echo "Landing warehouse tables from Postgres to Iceberg via Nessie"

docker exec -i de3-spark-master bash -c "
  mkdir -p /tmp/.ivy2 && \
  cd /opt/spark/work-dir && \
  export PYTHONPATH=/opt/spark/work-dir && \
  export SPARK_MASTER=spark://spark-master:7077 && \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.104.5 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.driver.extraJavaOptions='-Divy.cache.dir=/tmp/.ivy2 -Divy.home=/tmp/.ivy2' \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.wh=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.wh.catalog-impl=org.apache.iceberg.nessie.NessieCatalog \
    --conf spark.sql.catalog.wh.uri=http://nessie:19120/api/v1 \
    --conf spark.sql.catalog.wh.ref=main \
    --conf spark.sql.catalog.wh.warehouse=/opt/spark/work-dir/iceberg_warehouse \
    --conf spark.sql.catalog.wh.authentication.type=NONE \
    --conf spark.executor.memory=1g \
    --conf spark.driver.memory=1g \
    --conf spark.executor.cores=1 \
    --conf spark.cores.max=1 \
    --jars /opt/spark/work-dir/jars/postgresql.jar \
    /opt/spark/work-dir/spark_jobs/land_warehouse_to_iceberg.py
"

echo "✅ Warehouse landing to Iceberg via Nessie finished."