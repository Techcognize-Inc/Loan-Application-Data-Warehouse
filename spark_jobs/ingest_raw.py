import os

# SparkSession is the main entry point to use Spark
from pyspark.sql import SparkSession

# col() is used to refer to DataFrame columns
from pyspark.sql.functions import col

# These helper functions build JDBC connection details for Postgres
from spark_jobs.common.jdbc import jdbc_url, jdbc_properties


# Maps each source CSV file to the raw table name we want in Postgres
TABLE_MAP = {
    "application_train.csv": "application_train",
    "bureau.csv": "bureau",
    "bureau_balance.csv": "bureau_balance",
    "previous_application.csv": "previous_application",
    "POS_CASH_balance.csv": "pos_cash_balance",
    "installments_payments.csv": "installments_payments",
    "credit_card_balance.csv": "credit_card_balance",
}

# RAW_PATH = folder where raw CSV files are stored
# If RAW_PATH env variable is not set, use this default path
RAW_PATH = os.getenv("RAW_PATH", "/opt/spark/work-dir/data/raw")


def main():
    # Build Spark session configuration
    builder = (
        SparkSession.builder
        .appName("de3-ingest-raw")  # name shown in Spark UI / logs
        .config("spark.sql.adaptive.enabled", "true")  # lets Spark optimize some execution at runtime
    )

    # If SPARK_MASTER is passed as env variable, connect to that Spark cluster
    spark_master = os.getenv("SPARK_MASTER")
    if spark_master:
        builder = builder.master(spark_master)

    # Create Spark session
    spark = builder.getOrCreate()

    # Loop through each CSV file and its matching target table name
    for file_name, table_name in TABLE_MAP.items():
        # Build full path of the CSV file
        path = f"{RAW_PATH}/{file_name}"
        print(f"\n=== Reading: {path}")

        # Read CSV into Spark DataFrame
        # header=true -> first row is treated as column names
        # inferSchema=true -> Spark tries to detect data types automatically
        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )

        # Minimal cleanup:
        # trim spaces from column names, if any exist
        # Example: " SK_ID_CURR " -> "SK_ID_CURR"
        df = df.select([col(c).alias(c.strip()) for c in df.columns])

        # Target raw table name in Postgres
        # Example: raw.application_train
        full_table = f"raw.{table_name}"

        # Count total rows for logging / validation
        # Note: this is a Spark action, so it scans the DataFrame
        row_count = df.count()
        print(f"Writing to {full_table} | rows={row_count} | cols={len(df.columns)}")

        # Reduce DataFrame partitions before writing
        # coalesce(2) means use 2 output partitions for write
        # This helps keep JDBC write stable in our local environment
        write_df = df.coalesce(2)

        # Write DataFrame into Postgres using JDBC
        (
            write_df.write
            .mode("overwrite")  # replace table contents if table already exists
            .option("truncate", "true")  # try truncating before overwrite where possible
            .option("batchsize", "5000")  # insert rows in batches for better performance
            .option("isolationLevel", "NONE")  # reduce transaction overhead during bulk load
            .jdbc(
                url=jdbc_url(),               # JDBC URL for Postgres
                table=full_table,             # destination table name
                properties=jdbc_properties(), # user, password, driver
            )
        )

    # Stop Spark session after all files are loaded
    spark.stop()
    print("\n✅ Raw ingestion complete.")


# Standard Python entry point
if __name__ == "__main__":
    main()