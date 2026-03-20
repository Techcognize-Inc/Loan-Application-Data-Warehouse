import os

# SparkSession = main Spark entry point
from pyspark.sql import SparkSession

# Import aggregation / column functions used in transformations
from pyspark.sql.functions import col, count, avg, max as fmax, sum as fsum

# JDBC helpers to connect to Postgres
from spark_jobs.common.jdbc import jdbc_url, jdbc_properties


# -----------------------------
# Helpers
# -----------------------------

def quote_ident(name: str) -> str:
    # Adds double quotes around column names for Postgres safety
    # Example: SK_ID_CURR -> "SK_ID_CURR"
    return f'"{name}"'


def table_columns(spark: SparkSession, table: str) -> list[str]:
    # Reads table metadata and returns the list of columns in the table
    # Used to safely check whether expected columns actually exist
    return spark.read.jdbc(
        url=jdbc_url(),
        table=table,
        properties=jdbc_properties(),
    ).columns


def get_bounds(spark: SparkSession, table: str, partition_col: str) -> tuple[int, int]:
    # Finds MIN and MAX value of a partition column
    # This is used for partitioned JDBC reads
    q = f'(SELECT MIN({quote_ident(partition_col)}) AS lo, MAX({quote_ident(partition_col)}) AS hi FROM {table}) t'
    row = spark.read.jdbc(
        url=jdbc_url(),
        table=q,
        properties=jdbc_properties(),
    ).collect()[0]

    lo = row["lo"]
    hi = row["hi"]

    # If no values exist, return safe default bounds
    if lo is None or hi is None:
        return 0, 1

    return int(lo), int(hi)


def read_table_partitioned(
    spark: SparkSession,
    table: str,
    preferred_partition_cols: list[str],
    columns: list[str],
    num_partitions: int = 8,
):
    # Get actual columns present in the table
    cols_in_table = table_columns(spark, table)

    # Try to choose the first valid partition column from preferred list
    partition_col = None
    for c in preferred_partition_cols:
        if c in cols_in_table:
            partition_col = c
            break

    # If no partition column is found, fall back to normal JDBC read
    if partition_col is None:
        keep = [c for c in columns if c in cols_in_table]
        return (
            spark.read.jdbc(
                url=jdbc_url(),
                table=table,
                properties=jdbc_properties(),
            ).select(*keep)
        )

    # Select required columns plus partition column
    select_cols = list(dict.fromkeys(columns + [partition_col]))
    select_cols = [c for c in select_cols if c in cols_in_table]
    quoted_cols = ", ".join([quote_ident(c) for c in select_cols])

    # Build subquery so Spark reads only the required columns
    subquery = f'(SELECT {quoted_cols} FROM {table}) t'

    # Get min/max bounds for partitioned read
    lo, hi = get_bounds(spark, table, partition_col)

    # Partitioned JDBC read:
    # Spark splits the read into multiple chunks using partitionColumn
    return (
        spark.read.format("jdbc")
        .option("url", jdbc_url())
        .option("dbtable", subquery)
        .option("user", jdbc_properties().get("user"))
        .option("password", jdbc_properties().get("password"))
        .option("driver", jdbc_properties().get("driver"))
        .option("partitionColumn", partition_col)
        .option("lowerBound", lo)
        .option("upperBound", hi)
        .option("numPartitions", num_partitions)
        .load()
    )


def write_table_jdbc(df, table_name, mode="overwrite"):
    # Generic JDBC write helper
    (
        df.write
        .mode(mode)               # overwrite by default
        .option("batchsize", 5000)
        .jdbc(
            url=jdbc_url(),
            table=table_name,
            properties=jdbc_properties(),
        )
    )


# -----------------------------
# Main
# -----------------------------
def main():
    # Build Spark session for staging job
    builder = (
        SparkSession.builder
        .appName("de3-build-staging")
        .config("spark.sql.adaptive.enabled", "true")
    )

    # Use SPARK_MASTER if available
    spark_master = os.getenv("SPARK_MASTER")
    if spark_master:
        builder = builder.master(spark_master)

    # Create Spark session
    spark = builder.getOrCreate()

    # --------------------------------------------------
    # 1. Read base application table
    # This is the main/base dataset for staging
    # --------------------------------------------------
    app = read_table_partitioned(
        spark,
        table="raw.application_train",
        preferred_partition_cols=["SK_ID_CURR"],
        columns=[
            "SK_ID_CURR",
            "TARGET",
            "NAME_CONTRACT_TYPE",
            "CODE_GENDER",
            "FLAG_OWN_CAR",
            "FLAG_OWN_REALTY",
            "CNT_CHILDREN",
            "AMT_INCOME_TOTAL",
            "AMT_CREDIT",
            "AMT_ANNUITY",
            "AMT_GOODS_PRICE",
        ],
        num_partitions=8,
    )

    # --------------------------------------------------
    # 2. Read bureau data
    # Bureau is one-to-many with customer, so later we aggregate it
    # --------------------------------------------------
    bureau = read_table_partitioned(
        spark,
        table="raw.bureau",
        preferred_partition_cols=["SK_ID_BUREAU", "SK_ID_CURR"],
        columns=[
            "SK_ID_BUREAU",
            "SK_ID_CURR",
            "AMT_CREDIT_SUM",
            "CREDIT_DAY_OVERDUE",
        ],
        num_partitions=8,
    )

    # Aggregate bureau records to customer level
    bureau_agg = (
        bureau.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("bureau_records_cnt"),                        # total bureau records
            avg(col("AMT_CREDIT_SUM")).alias("bureau_avg_credit_sum"),     # avg credit sum
            fmax(col("AMT_CREDIT_SUM")).alias("bureau_max_credit_sum"),    # max credit sum
            avg(col("CREDIT_DAY_OVERDUE")).alias("bureau_avg_days_overdue"), # avg overdue days
            fmax(col("CREDIT_DAY_OVERDUE")).alias("bureau_max_days_overdue"), # max overdue days
        )
    )

    # --------------------------------------------------
    # 3. Read previous applications
    # --------------------------------------------------
    prev = read_table_partitioned(
        spark,
        table="raw.previous_application",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "AMT_APPLICATION",
            "AMT_CREDIT",
        ],
        num_partitions=8,
    )

    # Aggregate previous applications to customer level
    prev_agg = (
        prev.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("prev_app_cnt"),
            avg(col("AMT_APPLICATION")).alias("prev_avg_amt_application"),
            avg(col("AMT_CREDIT")).alias("prev_avg_amt_credit"),
        )
    )

    # --------------------------------------------------
    # 4. Read POS cash balance
    # POS = Point of Sale / cash loan history
    # --------------------------------------------------
    pos = read_table_partitioned(
        spark,
        table="raw.pos_cash_balance",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "SK_DPD",
        ],
        num_partitions=8,
    )

    # Aggregate POS data to customer level
    pos_agg = (
        pos.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("pos_records_cnt"),
            avg(col("SK_DPD")).alias("pos_avg_dpd"),
            fmax(col("SK_DPD")).alias("pos_max_dpd"),
        )
    )

    # --------------------------------------------------
    # 5. Read installments payments
    # --------------------------------------------------
    inst = read_table_partitioned(
        spark,
        table="raw.installments_payments",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "AMT_PAYMENT",
        ],
        num_partitions=8,
    )

    # Aggregate installment payments to customer level
    inst_agg = (
        inst.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("inst_records_cnt"),
            fsum(col("AMT_PAYMENT")).alias("inst_total_amt_paid"),
            avg(col("AMT_PAYMENT")).alias("inst_avg_amt_paid"),
        )
    )

    # --------------------------------------------------
    # 6. Read credit card balance
    # --------------------------------------------------
    cc = read_table_partitioned(
        spark,
        table="raw.credit_card_balance",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "AMT_BALANCE",
        ],
        num_partitions=8,
    )

    # Aggregate credit card data to customer level
    cc_agg = (
        cc.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("cc_records_cnt"),
            avg(col("AMT_BALANCE")).alias("cc_avg_balance"),
            fmax(col("AMT_BALANCE")).alias("cc_max_balance"),
        )
    )

    # --------------------------------------------------
    # 7. Join all aggregated datasets back to application base table
    # left join = keep all base application rows even if some side data is missing
    # --------------------------------------------------
    stg = (
        app
        .join(bureau_agg, on="SK_ID_CURR", how="left")
        .join(prev_agg, on="SK_ID_CURR", how="left")
        .join(pos_agg, on="SK_ID_CURR", how="left")
        .join(inst_agg, on="SK_ID_CURR", how="left")
        .join(cc_agg, on="SK_ID_CURR", how="left")
    )

    # Keep important base columns from application table
    base_keep = [
        "SK_ID_CURR",
        "TARGET",
        "NAME_CONTRACT_TYPE",
        "CODE_GENDER",
        "FLAG_OWN_CAR",
        "FLAG_OWN_REALTY",
        "CNT_CHILDREN",
        "AMT_INCOME_TOTAL",
        "AMT_CREDIT",
        "AMT_ANNUITY",
        "AMT_GOODS_PRICE",
    ]

    # Only keep columns that actually exist in the joined DataFrame
    existing_keep = [c for c in base_keep if c in stg.columns]

    # These are the newly created aggregated feature columns
    agg_cols = [c for c in stg.columns if c not in app.columns]

    # Final selected staging DataFrame
    final_df = stg.select(*(existing_keep + agg_cols))

    # Repartition before write to keep output more balanced and stable
    final_df = final_df.repartition(4, col("SK_ID_CURR"))

    # Write final enriched dataset to staging schema
    write_table_jdbc(final_df, "staging.stg_loan_application_enriched", mode="overwrite")

    # Stop Spark session
    spark.stop()
    print("✅ Staging table created: staging.stg_loan_application_enriched")


# Standard Python entry point
if __name__ == "__main__":
    main()