import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as fmax, sum as fsum, lit, when

from spark_jobs.common.jdbc import jdbc_url, jdbc_properties


# -----------------------------
# Helpers
# -----------------------------
def quote_ident(name: str) -> str:
    return f'"{name}"'


def table_columns(spark: SparkSession, table: str) -> list[str]:
    return spark.read.jdbc(
        url=jdbc_url(),
        table=table,
        properties=jdbc_properties(),
    ).columns


def get_bounds(spark: SparkSession, table: str, partition_col: str) -> tuple[int, int]:
    q = f'(SELECT MIN({quote_ident(partition_col)}) AS lo, MAX({quote_ident(partition_col)}) AS hi FROM {table}) t'
    row = spark.read.jdbc(
        url=jdbc_url(),
        table=q,
        properties=jdbc_properties(),
    ).collect()[0]

    lo = row["lo"]
    hi = row["hi"]

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
    cols_in_table = table_columns(spark, table)

    partition_col = None
    for c in preferred_partition_cols:
        if c in cols_in_table:
            partition_col = c
            break

    if partition_col is None:
        keep = [c for c in columns if c in cols_in_table]
        return (
            spark.read.jdbc(
                url=jdbc_url(),
                table=table,
                properties=jdbc_properties(),
            ).select(*keep)
        )

    select_cols = list(dict.fromkeys(columns + [partition_col]))
    select_cols = [c for c in select_cols if c in cols_in_table]
    quoted_cols = ", ".join([quote_ident(c) for c in select_cols])

    subquery = f'(SELECT {quoted_cols} FROM {table}) t'
    lo, hi = get_bounds(spark, table, partition_col)

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
    (
        df.write
        .mode(mode)
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
    builder = (
        SparkSession.builder
        .appName("de3-build-staging")
        .config("spark.sql.adaptive.enabled", "true")
    )

    spark_master = os.getenv("SPARK_MASTER")
    if spark_master:
        builder = builder.master(spark_master)

    spark = builder.getOrCreate()

    # Base application table (NOW reading from silver)
    app = read_table_partitioned(
        spark,
        table="silver.application_train",
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

    # Bureau (NOW reading from silver)
    bureau = read_table_partitioned(
        spark,
        table="silver.bureau",
        preferred_partition_cols=["SK_ID_BUREAU", "SK_ID_CURR"],
        columns=[
            "SK_ID_BUREAU",
            "SK_ID_CURR",
            "AMT_CREDIT_SUM",
            "CREDIT_DAY_OVERDUE",
        ],
        num_partitions=8,
    )

    bureau_agg = (
        bureau.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("bureau_records_cnt"),
            avg(col("AMT_CREDIT_SUM")).alias("bureau_avg_credit_sum"),
            fmax(col("AMT_CREDIT_SUM")).alias("bureau_max_credit_sum"),
            avg(col("CREDIT_DAY_OVERDUE")).alias("bureau_avg_days_overdue"),
            fmax(col("CREDIT_DAY_OVERDUE")).alias("bureau_max_days_overdue"),
        )
    )

    # Previous applications (NOW reading from silver)
    prev = read_table_partitioned(
        spark,
        table="silver.previous_application",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "AMT_APPLICATION",
            "AMT_CREDIT",
        ],
        num_partitions=8,
    )

    prev_agg = (
        prev.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("prev_app_cnt"),
            avg(col("AMT_APPLICATION")).alias("prev_avg_amt_application"),
            avg(col("AMT_CREDIT")).alias("prev_avg_amt_credit"),
        )
    )

    # POS cash balance (NOW reading from silver)
    pos = read_table_partitioned(
        spark,
        table="silver.pos_cash_balance",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "SK_DPD",
        ],
        num_partitions=8,
    )

    pos_agg = (
        pos.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("pos_records_cnt"),
            avg(col("SK_DPD")).alias("pos_avg_dpd"),
            fmax(col("SK_DPD")).alias("pos_max_dpd"),
        )
    )

    # Installments payments (NOW reading from silver)
    inst = read_table_partitioned(
        spark,
        table="silver.installments_payments",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "AMT_PAYMENT",
        ],
        num_partitions=8,
    )

    inst_agg = (
        inst.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("inst_records_cnt"),
            fsum(col("AMT_PAYMENT")).alias("inst_total_amt_paid"),
            avg(col("AMT_PAYMENT")).alias("inst_avg_amt_paid"),
        )
    )

    # Credit card balance (NOW reading from silver)
    cc = read_table_partitioned(
        spark,
        table="silver.credit_card_balance",
        preferred_partition_cols=["SK_ID_PREV", "SK_ID_CURR"],
        columns=[
            "SK_ID_PREV",
            "SK_ID_CURR",
            "AMT_BALANCE",
        ],
        num_partitions=8,
    )

    cc_agg = (
        cc.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("cc_records_cnt"),
            avg(col("AMT_BALANCE")).alias("cc_avg_balance"),
            fmax(col("AMT_BALANCE")).alias("cc_max_balance"),
        )
    )

    # Join all aggregates back to application
    stg = (
        app
        .join(bureau_agg, on="SK_ID_CURR", how="left")
        .join(prev_agg, on="SK_ID_CURR", how="left")
        .join(pos_agg, on="SK_ID_CURR", how="left")
        .join(inst_agg, on="SK_ID_CURR", how="left")
        .join(cc_agg, on="SK_ID_CURR", how="left")
    )

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
    existing_keep = [c for c in base_keep if c in stg.columns]
    agg_cols = [c for c in stg.columns if c not in app.columns]

    final_df = stg.select(*(existing_keep + agg_cols))

    # ------------------------------------------
    # Final important staging checks only
    # ------------------------------------------
    reject_reason_expr = (
        when(col("SK_ID_CURR").isNull(), lit("NULL_SK_ID_CURR"))
        .when(col("TARGET").isNull(), lit("NULL_TARGET"))
        .when(~col("TARGET").isin(0, 1), lit("INVALID_TARGET"))
        .when(col("AMT_CREDIT").isNull(), lit("NULL_AMT_CREDIT"))
        .when(col("AMT_INCOME_TOTAL").isNull(), lit("NULL_AMT_INCOME_TOTAL"))
    )

    rejects_df = final_df.filter(
        col("SK_ID_CURR").isNull() |
        col("TARGET").isNull() |
        (~col("TARGET").isin(0, 1)) |
        col("AMT_CREDIT").isNull() |
        col("AMT_INCOME_TOTAL").isNull()
    ).withColumn("reject_reason", reject_reason_expr)

    valid_df = final_df.filter(
        col("SK_ID_CURR").isNotNull() &
        col("TARGET").isNotNull() &
        col("TARGET").isin(0, 1) &
        col("AMT_CREDIT").isNotNull() &
        col("AMT_INCOME_TOTAL").isNotNull()
    )

    # Keep write stable
    valid_df = valid_df.repartition(4, col("SK_ID_CURR"))

    # Write curated staging rows
    write_table_jdbc(valid_df, "staging.stg_loan_application_enriched", mode="overwrite")

    # Write staging rejects if any
    reject_count = rejects_df.count()
    if reject_count > 0:
        write_table_jdbc(
            rejects_df.coalesce(1),
            "quarantine.stg_loan_application_enriched_rejects",
            mode="overwrite",
        )

    spark.stop()
    print("✅ Staging table created: staging.stg_loan_application_enriched")
    print(f"✅ Staging rejected rows written: {reject_count}")


if __name__ == "__main__":
    main()