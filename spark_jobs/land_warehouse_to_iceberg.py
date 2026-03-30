import os
from pyspark.sql import SparkSession

from spark_jobs.common.jdbc import jdbc_url, jdbc_properties

TABLES = [
    "fact_loans",
    "dim_customer",
    "dim_bureau",
]

POSTGRES_SCHEMA = os.getenv("PG_WAREHOUSE_SCHEMA", "warehouse")
ICEBERG_NAMESPACE = os.getenv("ICEBERG_NAMESPACE", "warehouse")  # iceberg.<namespace>.<table>


def main():
    spark = (
        SparkSession.builder
        .appName("de3-land-warehouse-to-iceberg")
        .getOrCreate()
    )

    # Make sure namespace exists in Iceberg
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{ICEBERG_NAMESPACE}")

    for t in TABLES:
        src = f"{POSTGRES_SCHEMA}.{t}"
        dest = f"iceberg.{ICEBERG_NAMESPACE}.{t}"

        print(f"\n=== Landing {src}  ->  {dest}")

        df = spark.read.jdbc(
            url=jdbc_url(),
            table=src,
            properties=jdbc_properties(),
        )

        # Create or replace Iceberg table
        (
            df.writeTo(dest)
            .using("iceberg")
            .createOrReplace()
        )

        print(f"✅ Done: {dest}")

    spark.stop()
    print("\n✅ All warehouse tables landed to Iceberg.")


if __name__ == "__main__":
    main()