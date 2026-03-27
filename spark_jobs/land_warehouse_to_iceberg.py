import os

from pyspark.sql import SparkSession


def read_postgres_table(spark, table_name: str):
    return (
        spark.read.format("jdbc")
        .option("url", "jdbc:postgresql://postgres:5432/bankingdb")
        .option("dbtable", table_name)
        .option("user", "postgres")
        .option("password", "password")
        .option("driver", "org.postgresql.Driver")
        .load()
    )


def main():
    builder = (
        SparkSession.builder
        .appName("de3-land-warehouse-to-iceberg")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.wh", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.wh.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.wh.uri", "http://host.docker.internal:19120/api/v1")
        .config("spark.sql.catalog.wh.ref", "main")
        .config("spark.sql.catalog.wh.warehouse", "/opt/spark/work-dir/iceberg_warehouse")
        .config("spark.sql.catalog.wh.authentication.type", "NONE")
    )

    spark_master = os.getenv("SPARK_MASTER")
    if spark_master:
        builder = builder.master(spark_master)

    spark = builder.getOrCreate()

    # Create namespace if not exists
    spark.sql("CREATE NAMESPACE IF NOT EXISTS wh.warehouse")

    # Read warehouse tables from Postgres
    fact_loans_df = read_postgres_table(spark, "warehouse.fact_loans")
    dim_customer_df = read_postgres_table(spark, "warehouse.dim_customer")
    dim_bureau_df = read_postgres_table(spark, "warehouse.dim_bureau")

    # Write to Iceberg
    fact_loans_df.writeTo("wh.warehouse.fact_loans").createOrReplace()
    dim_customer_df.writeTo("wh.warehouse.dim_customer").createOrReplace()
    dim_bureau_df.writeTo("wh.warehouse.dim_bureau").createOrReplace()

    print("✅ Landed warehouse.fact_loans to Iceberg")
    print("✅ Landed warehouse.dim_customer to Iceberg")
    print("✅ Landed warehouse.dim_bureau to Iceberg")

    spark.stop()


if __name__ == "__main__":
    main()