import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

from spark_jobs.common.jdbc import jdbc_url, jdbc_properties


TABLE_MAP = {
    "application_train.csv": "application_train",
    "bureau.csv": "bureau",
    "bureau_balance.csv": "bureau_balance",
    "previous_application.csv": "previous_application",
    "POS_CASH_balance.csv": "pos_cash_balance",
    "installments_payments.csv": "installments_payments",
    "credit_card_balance.csv": "credit_card_balance",
}

RAW_PATH = os.getenv("RAW_PATH", "/opt/spark/work-dir/data/raw")


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
        print(f"\n=== Reading: {path}")

        df = (
            spark.read
            .option("header", "true")
            .option("inferSchema", "true")
            .csv(path)
        )

        # Clean only column names, not values
        df = df.select([col(c).alias(c.strip()) for c in df.columns])

        raw_table = f"raw.{table_name}"
        quarantine_table = f"quarantine.{table_name}_rejects"

        # ------------------------------------------
        # Main strong validation for application_train
        # ------------------------------------------
        if table_name == "application_train":
            required_cols = [
                "SK_ID_CURR",
                "TARGET",
                "AMT_CREDIT",
                "AMT_INCOME_TOTAL",
                "AMT_ANNUITY",
                "AMT_GOODS_PRICE",
            ]

            missing_cols = [c for c in required_cols if c not in df.columns]
            if missing_cols:
                raise Exception(f"Missing required columns in {file_name}: {missing_cols}")

            # Explicit casts for critical columns so we can catch bad values like 'abc'
            df_casted = (
                df
                .withColumn("SK_ID_CURR_INT", col("SK_ID_CURR").cast("int"))
                .withColumn("TARGET_INT", col("TARGET").cast("int"))
                .withColumn("AMT_CREDIT_DBL", col("AMT_CREDIT").cast("double"))
                .withColumn("AMT_INCOME_TOTAL_DBL", col("AMT_INCOME_TOTAL").cast("double"))
                .withColumn("AMT_ANNUITY_DBL", col("AMT_ANNUITY").cast("double"))
                .withColumn("AMT_GOODS_PRICE_DBL", col("AMT_GOODS_PRICE").cast("double"))
            )

            reject_reason_expr = (
                when(col("SK_ID_CURR").isNull(), lit("NULL_SK_ID_CURR"))
                .when(
                    col("SK_ID_CURR").isNotNull() & col("SK_ID_CURR_INT").isNull(),
                    lit("INVALID_SK_ID_CURR_TYPE")
                )
                .when(col("TARGET").isNull(), lit("NULL_TARGET"))
                .when(
                    col("TARGET").isNotNull() & col("TARGET_INT").isNull(),
                    lit("INVALID_TARGET_TYPE")
                )
                .when(
                    col("TARGET_INT").isNotNull() & (~col("TARGET_INT").isin(0, 1)),
                    lit("INVALID_TARGET")
                )
                .when(
                    col("AMT_CREDIT").isNull(),
                    lit("NULL_AMT_CREDIT")
                )
                .when(
                    col("AMT_CREDIT").isNotNull() & col("AMT_CREDIT_DBL").isNull(),
                    lit("INVALID_AMT_CREDIT")
                )
                .when(
                    col("AMT_INCOME_TOTAL").isNull(),
                    lit("NULL_AMT_INCOME_TOTAL")
                )
                .when(
                    col("AMT_INCOME_TOTAL").isNotNull() & col("AMT_INCOME_TOTAL_DBL").isNull(),
                    lit("INVALID_AMT_INCOME_TOTAL")
                )
                .when(
                    col("AMT_ANNUITY").isNotNull() & col("AMT_ANNUITY_DBL").isNull(),
                    lit("INVALID_AMT_ANNUITY")
                )
                .when(
                    col("AMT_GOODS_PRICE").isNotNull() & col("AMT_GOODS_PRICE_DBL").isNull(),
                    lit("INVALID_AMT_GOODS_PRICE")
                )
            )

            rejects_df = df_casted.filter(
                col("SK_ID_CURR").isNull() |
                (col("SK_ID_CURR").isNotNull() & col("SK_ID_CURR_INT").isNull()) |
                col("TARGET").isNull() |
                (col("TARGET").isNotNull() & col("TARGET_INT").isNull()) |
                (col("TARGET_INT").isNotNull() & (~col("TARGET_INT").isin(0, 1))) |
                col("AMT_CREDIT").isNull() |
                (col("AMT_CREDIT").isNotNull() & col("AMT_CREDIT_DBL").isNull()) |
                col("AMT_INCOME_TOTAL").isNull() |
                (col("AMT_INCOME_TOTAL").isNotNull() & col("AMT_INCOME_TOTAL_DBL").isNull()) |
                (col("AMT_ANNUITY").isNotNull() & col("AMT_ANNUITY_DBL").isNull()) |
                (col("AMT_GOODS_PRICE").isNotNull() & col("AMT_GOODS_PRICE_DBL").isNull())
            ).withColumn("reject_reason", reject_reason_expr)

            valid_df = df_casted.filter(
                col("SK_ID_CURR_INT").isNotNull() &
                col("TARGET_INT").isNotNull() &
                col("TARGET_INT").isin(0, 1) &
                col("AMT_CREDIT_DBL").isNotNull() &
                col("AMT_INCOME_TOTAL_DBL").isNotNull()
            )

            # Replace original critical columns with validated typed versions
            valid_df = (
                valid_df
                .drop("SK_ID_CURR", "TARGET", "AMT_CREDIT", "AMT_INCOME_TOTAL", "AMT_ANNUITY", "AMT_GOODS_PRICE")
                .withColumnRenamed("SK_ID_CURR_INT", "SK_ID_CURR")
                .withColumnRenamed("TARGET_INT", "TARGET")
                .withColumnRenamed("AMT_CREDIT_DBL", "AMT_CREDIT")
                .withColumnRenamed("AMT_INCOME_TOTAL_DBL", "AMT_INCOME_TOTAL")
                .withColumnRenamed("AMT_ANNUITY_DBL", "AMT_ANNUITY")
                .withColumnRenamed("AMT_GOODS_PRICE_DBL", "AMT_GOODS_PRICE")
            )

            # Drop helper cast columns from rejects before writing quarantine
            helper_cols = [
                "SK_ID_CURR_INT",
                "TARGET_INT",
                "AMT_CREDIT_DBL",
                "AMT_INCOME_TOTAL_DBL",
                "AMT_ANNUITY_DBL",
                "AMT_GOODS_PRICE_DBL",
            ]
            for c in helper_cols:
                if c in rejects_df.columns:
                    rejects_df = rejects_df.drop(c)

        # ------------------------------------------
        # Lighter validation for the remaining files
        # ------------------------------------------
        else:
            if "SK_ID_CURR" in df.columns:
                rejects_df = (
                    df.filter(col("SK_ID_CURR").isNull())
                    .withColumn("reject_reason", lit("NULL_SK_ID_CURR"))
                )
                valid_df = df.filter(col("SK_ID_CURR").isNotNull())
            else:
                rejects_df = None
                valid_df = df

        # ------------------------------------------
        # Logging
        # ------------------------------------------
        valid_count = valid_df.count()
        reject_count = rejects_df.count() if rejects_df is not None else 0

        print(
            f"Writing valid rows to {raw_table} | valid_rows={valid_count} | "
            f"reject_rows={reject_count} | cols={len(valid_df.columns)}"
        )

        # Write valid rows to raw
        write_jdbc(valid_df.coalesce(2), raw_table, mode="overwrite")

        # Write rejected rows to quarantine if any exist
        if rejects_df is not None and reject_count > 0:
            print(f"Writing rejected rows to {quarantine_table}")
            write_jdbc(rejects_df.coalesce(1), quarantine_table, mode="overwrite")

    spark.stop()
    print("\n✅ Raw ingestion complete with schema + data quality validation.")


if __name__ == "__main__":
    main()