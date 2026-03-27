import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col

from spark_jobs.common.jdbc import jdbc_url, jdbc_properties


# ---------------------------------------------------------------------------
# File → raw table mapping
# ---------------------------------------------------------------------------
TABLE_MAP = {
    "application_train.csv":    "application_train",
    "bureau.csv":               "bureau",
    "bureau_balance.csv":       "bureau_balance",
    "previous_application.csv": "previous_application",
    "POS_CASH_balance.csv":     "pos_cash_balance",
    "installments_payments.csv":"installments_payments",
    "credit_card_balance.csv":  "credit_card_balance",
}

RAW_PATH = os.getenv("RAW_PATH", "/opt/spark/work-dir/data/raw")


# ---------------------------------------------------------------------------
# JDBC writer
# ---------------------------------------------------------------------------
def write_jdbc(df, table_name, mode="overwrite"):
    (
        df.write
        .mode(mode)
        .option("truncate", "true")
        .option("batchsize", "5000")
        .option("isolationLevel", "NONE")
        .jdbc(
            url=jdbc_url(),
            table=table_name,
            properties=jdbc_properties(),
        )
    )


# ---------------------------------------------------------------------------
# Main — ingest ONLY, zero DQ, zero rejection
# ---------------------------------------------------------------------------
def main():
    builder = (
        SparkSession.builder
        .appName("de3-ingest-raw")
        .config("spark.sql.adaptive.enabled", "true")
    )

    spark_master = os.getenv("SPARK_MASTER")
    if spark_master:
        builder = builder.master(spark_master)

    spark = builder.getOrCreate()

    for file_name, table_name in TABLE_MAP.items():
        path = f"{RAW_PATH}/{file_name}"
        print(f"\n=== Ingesting: {path}")

        # Read CSV as-is — no schema enforcement, no validation
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )

        # Only minimal transform: strip leading/trailing spaces from column names
        df = df.select([col(c).alias(c.strip()) for c in df.columns])

        raw_table = f"raw.{table_name}"
        row_count = df.count()

        print(f"Writing {row_count} rows → {raw_table} | cols={len(df.columns)}")

        # Land everything into raw — ALL rows, no filtering
        write_jdbc(df.coalesce(2), raw_table, mode="overwrite")

        print(f"✅ Done: {raw_table}")

    spark.stop()
    print("\n✅ Raw ingestion complete — all rows landed, no DQ applied.")


if __name__ == "__main__":
    main()