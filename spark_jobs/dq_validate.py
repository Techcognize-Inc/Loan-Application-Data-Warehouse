import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when

from spark_jobs.common.jdbc import jdbc_url, jdbc_properties


# ---------------------------------------------------------------------------
# Tables to validate — raw → silver
# ---------------------------------------------------------------------------
TABLES = [
    "application_train",
    "bureau",
    "bureau_balance",
    "previous_application",
    "pos_cash_balance",
    "installments_payments",
    "credit_card_balance",
]


# ---------------------------------------------------------------------------
# JDBC helpers
# ---------------------------------------------------------------------------
def read_jdbc(spark: SparkSession, table: str) -> DataFrame:
    return spark.read.jdbc(
        url=jdbc_url(),
        table=table,
        properties=jdbc_properties(),
    )


def write_jdbc(df: DataFrame, table_name: str, mode: str = "overwrite"):
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
# DQ rules per table
# ---------------------------------------------------------------------------

def validate_application_train(df: DataFrame):
    """
    Strong validation on the primary loan application table.
    Checks:
      - SK_ID_CURR      : not null, castable to int
      - TARGET          : not null, castable to int, value in {0, 1}
      - AMT_CREDIT      : not null, castable to double
      - AMT_INCOME_TOTAL: not null, castable to double
      - AMT_ANNUITY     : castable to double if present
      - AMT_GOODS_PRICE : castable to double if present
      - CODE_GENDER     : value in {M, F, XNA} if present
      - AMT_INCOME_TOTAL: > 0 (range check)
      - AMT_CREDIT      : > 0 (range check)
      - SK_ID_CURR      : no duplicates flagged via reject_reason
    """

    # --- Cast helpers ---
    df_casted = (
        df
        .withColumn("SK_ID_CURR_INT",        col("SK_ID_CURR").cast("int"))
        .withColumn("TARGET_INT",             col("TARGET").cast("int"))
        .withColumn("AMT_CREDIT_DBL",         col("AMT_CREDIT").cast("double"))
        .withColumn("AMT_INCOME_TOTAL_DBL",   col("AMT_INCOME_TOTAL").cast("double"))
        .withColumn("AMT_ANNUITY_DBL",        col("AMT_ANNUITY").cast("double"))
        .withColumn("AMT_GOODS_PRICE_DBL",    col("AMT_GOODS_PRICE").cast("double"))
    )

    # --- Reject reason expression (first failing rule wins) ---
    reject_reason_expr = (
        when(col("SK_ID_CURR").isNull(),                                            lit("NULL_SK_ID_CURR"))
        .when(col("SK_ID_CURR_INT").isNull(),                                       lit("INVALID_SK_ID_CURR_TYPE"))
        .when(col("TARGET").isNull(),                                               lit("NULL_TARGET"))
        .when(col("TARGET_INT").isNull(),                                           lit("INVALID_TARGET_TYPE"))
        .when(~col("TARGET_INT").isin(0, 1),                                        lit("INVALID_TARGET_VALUE"))
        .when(col("AMT_CREDIT").isNull(),                                           lit("NULL_AMT_CREDIT"))
        .when(col("AMT_CREDIT_DBL").isNull(),                                       lit("INVALID_AMT_CREDIT_TYPE"))
        .when(col("AMT_CREDIT_DBL") <= 0,                                           lit("NON_POSITIVE_AMT_CREDIT"))
        .when(col("AMT_INCOME_TOTAL").isNull(),                                     lit("NULL_AMT_INCOME_TOTAL"))
        .when(col("AMT_INCOME_TOTAL_DBL").isNull(),                                 lit("INVALID_AMT_INCOME_TOTAL_TYPE"))
        .when(col("AMT_INCOME_TOTAL_DBL") <= 0,                                     lit("NON_POSITIVE_AMT_INCOME_TOTAL"))
        .when(col("AMT_ANNUITY").isNotNull() & col("AMT_ANNUITY_DBL").isNull(),     lit("INVALID_AMT_ANNUITY_TYPE"))
        .when(col("AMT_GOODS_PRICE").isNotNull() & col("AMT_GOODS_PRICE_DBL").isNull(), lit("INVALID_AMT_GOODS_PRICE_TYPE"))
        .when(
            col("CODE_GENDER").isNotNull() & (~col("CODE_GENDER").isin("M", "F", "XNA")),
            lit("INVALID_CODE_GENDER")
        )
    )

    # --- Reject filter ---
    reject_filter = (
        col("SK_ID_CURR").isNull() |
        col("SK_ID_CURR_INT").isNull() |
        col("TARGET").isNull() |
        col("TARGET_INT").isNull() |
        (~col("TARGET_INT").isin(0, 1)) |
        col("AMT_CREDIT").isNull() |
        col("AMT_CREDIT_DBL").isNull() |
        (col("AMT_CREDIT_DBL") <= 0) |
        col("AMT_INCOME_TOTAL").isNull() |
        col("AMT_INCOME_TOTAL_DBL").isNull() |
        (col("AMT_INCOME_TOTAL_DBL") <= 0) |
        (col("AMT_ANNUITY").isNotNull() & col("AMT_ANNUITY_DBL").isNull()) |
        (col("AMT_GOODS_PRICE").isNotNull() & col("AMT_GOODS_PRICE_DBL").isNull()) |
        (col("CODE_GENDER").isNotNull() & (~col("CODE_GENDER").isin("M", "F", "XNA")))
    )

    # --- Split ---
    helper_cols = [
        "SK_ID_CURR_INT", "TARGET_INT", "AMT_CREDIT_DBL",
        "AMT_INCOME_TOTAL_DBL", "AMT_ANNUITY_DBL", "AMT_GOODS_PRICE_DBL",
    ]

    rejects_df = (
        df_casted
        .filter(reject_filter)
        .withColumn("reject_reason", reject_reason_expr)
        .drop(*helper_cols)
    )

    valid_df = (
        df_casted
        .filter(~reject_filter)
        # Replace string cols with validated typed versions
        .drop("SK_ID_CURR", "TARGET", "AMT_CREDIT", "AMT_INCOME_TOTAL", "AMT_ANNUITY", "AMT_GOODS_PRICE")
        .withColumnRenamed("SK_ID_CURR_INT",       "SK_ID_CURR")
        .withColumnRenamed("TARGET_INT",            "TARGET")
        .withColumnRenamed("AMT_CREDIT_DBL",        "AMT_CREDIT")
        .withColumnRenamed("AMT_INCOME_TOTAL_DBL",  "AMT_INCOME_TOTAL")
        .withColumnRenamed("AMT_ANNUITY_DBL",       "AMT_ANNUITY")
        .withColumnRenamed("AMT_GOODS_PRICE_DBL",   "AMT_GOODS_PRICE")
    )

    return valid_df, rejects_df


def validate_bureau(df: DataFrame):
    """
    Checks:
      - SK_ID_CURR   : not null, castable to int
      - SK_ID_BUREAU : not null, castable to int
      - AMT_CREDIT_SUM      : castable to double if present
      - CREDIT_DAY_OVERDUE  : castable to double if present, >= 0
    """
    df_casted = (
        df
        .withColumn("SK_ID_CURR_INT",          col("SK_ID_CURR").cast("int"))
        .withColumn("SK_ID_BUREAU_INT",         col("SK_ID_BUREAU").cast("int"))
        .withColumn("AMT_CREDIT_SUM_DBL",       col("AMT_CREDIT_SUM").cast("double"))
        .withColumn("CREDIT_DAY_OVERDUE_DBL",   col("CREDIT_DAY_OVERDUE").cast("double"))
    )

    reject_reason_expr = (
        when(col("SK_ID_CURR").isNull(),                                              lit("NULL_SK_ID_CURR"))
        .when(col("SK_ID_CURR_INT").isNull(),                                         lit("INVALID_SK_ID_CURR_TYPE"))
        .when(col("SK_ID_BUREAU").isNull(),                                           lit("NULL_SK_ID_BUREAU"))
        .when(col("SK_ID_BUREAU_INT").isNull(),                                       lit("INVALID_SK_ID_BUREAU_TYPE"))
        .when(col("AMT_CREDIT_SUM").isNotNull() & col("AMT_CREDIT_SUM_DBL").isNull(), lit("INVALID_AMT_CREDIT_SUM_TYPE"))
        .when(
            col("CREDIT_DAY_OVERDUE").isNotNull() & col("CREDIT_DAY_OVERDUE_DBL").isNull(),
            lit("INVALID_CREDIT_DAY_OVERDUE_TYPE")
        )
        .when(
            col("CREDIT_DAY_OVERDUE_DBL").isNotNull() & (col("CREDIT_DAY_OVERDUE_DBL") < 0),
            lit("NEGATIVE_CREDIT_DAY_OVERDUE")
        )
    )

    reject_filter = (
        col("SK_ID_CURR").isNull() |
        col("SK_ID_CURR_INT").isNull() |
        col("SK_ID_BUREAU").isNull() |
        col("SK_ID_BUREAU_INT").isNull() |
        (col("AMT_CREDIT_SUM").isNotNull() & col("AMT_CREDIT_SUM_DBL").isNull()) |
        (col("CREDIT_DAY_OVERDUE").isNotNull() & col("CREDIT_DAY_OVERDUE_DBL").isNull()) |
        (col("CREDIT_DAY_OVERDUE_DBL").isNotNull() & (col("CREDIT_DAY_OVERDUE_DBL") < 0))
    )

    helper_cols = ["SK_ID_CURR_INT", "SK_ID_BUREAU_INT", "AMT_CREDIT_SUM_DBL", "CREDIT_DAY_OVERDUE_DBL"]

    rejects_df = df_casted.filter(reject_filter).withColumn("reject_reason", reject_reason_expr).drop(*helper_cols)
    valid_df   = df_casted.filter(~reject_filter).drop(*helper_cols)

    return valid_df, rejects_df


def validate_bureau_balance(df: DataFrame):
    """
    Checks:
      - SK_ID_BUREAU : not null, castable to int
      - MONTHS_BALANCE: not null, castable to int
      - STATUS       : not null
    """
    df_casted = (
        df
        .withColumn("SK_ID_BUREAU_INT",    col("SK_ID_BUREAU").cast("int"))
        .withColumn("MONTHS_BALANCE_INT",  col("MONTHS_BALANCE").cast("int"))
    )

    reject_reason_expr = (
        when(col("SK_ID_BUREAU").isNull(),                lit("NULL_SK_ID_BUREAU"))
        .when(col("SK_ID_BUREAU_INT").isNull(),           lit("INVALID_SK_ID_BUREAU_TYPE"))
        .when(col("MONTHS_BALANCE").isNull(),             lit("NULL_MONTHS_BALANCE"))
        .when(col("MONTHS_BALANCE_INT").isNull(),         lit("INVALID_MONTHS_BALANCE_TYPE"))
        .when(col("STATUS").isNull(),                     lit("NULL_STATUS"))
    )

    reject_filter = (
        col("SK_ID_BUREAU").isNull() |
        col("SK_ID_BUREAU_INT").isNull() |
        col("MONTHS_BALANCE").isNull() |
        col("MONTHS_BALANCE_INT").isNull() |
        col("STATUS").isNull()
    )

    helper_cols = ["SK_ID_BUREAU_INT", "MONTHS_BALANCE_INT"]

    rejects_df = df_casted.filter(reject_filter).withColumn("reject_reason", reject_reason_expr).drop(*helper_cols)
    valid_df   = df_casted.filter(~reject_filter).drop(*helper_cols)

    return valid_df, rejects_df


def validate_previous_application(df: DataFrame):
    """
    Checks:
      - SK_ID_CURR   : not null, castable to int
      - SK_ID_PREV   : not null, castable to int
      - AMT_APPLICATION: castable to double if present, >= 0
      - AMT_CREDIT     : castable to double if present, >= 0
    """
    df_casted = (
        df
        .withColumn("SK_ID_CURR_INT",       col("SK_ID_CURR").cast("int"))
        .withColumn("SK_ID_PREV_INT",        col("SK_ID_PREV").cast("int"))
        .withColumn("AMT_APPLICATION_DBL",   col("AMT_APPLICATION").cast("double"))
        .withColumn("AMT_CREDIT_DBL",        col("AMT_CREDIT").cast("double"))
    )

    reject_reason_expr = (
        when(col("SK_ID_CURR").isNull(),                                              lit("NULL_SK_ID_CURR"))
        .when(col("SK_ID_CURR_INT").isNull(),                                         lit("INVALID_SK_ID_CURR_TYPE"))
        .when(col("SK_ID_PREV").isNull(),                                             lit("NULL_SK_ID_PREV"))
        .when(col("SK_ID_PREV_INT").isNull(),                                         lit("INVALID_SK_ID_PREV_TYPE"))
        .when(col("AMT_APPLICATION").isNotNull() & col("AMT_APPLICATION_DBL").isNull(), lit("INVALID_AMT_APPLICATION_TYPE"))
        .when(col("AMT_APPLICATION_DBL").isNotNull() & (col("AMT_APPLICATION_DBL") < 0), lit("NEGATIVE_AMT_APPLICATION"))
        .when(col("AMT_CREDIT").isNotNull() & col("AMT_CREDIT_DBL").isNull(),         lit("INVALID_AMT_CREDIT_TYPE"))
        .when(col("AMT_CREDIT_DBL").isNotNull() & (col("AMT_CREDIT_DBL") < 0),        lit("NEGATIVE_AMT_CREDIT"))
    )

    reject_filter = (
        col("SK_ID_CURR").isNull() |
        col("SK_ID_CURR_INT").isNull() |
        col("SK_ID_PREV").isNull() |
        col("SK_ID_PREV_INT").isNull() |
        (col("AMT_APPLICATION").isNotNull() & col("AMT_APPLICATION_DBL").isNull()) |
        (col("AMT_APPLICATION_DBL").isNotNull() & (col("AMT_APPLICATION_DBL") < 0)) |
        (col("AMT_CREDIT").isNotNull() & col("AMT_CREDIT_DBL").isNull()) |
        (col("AMT_CREDIT_DBL").isNotNull() & (col("AMT_CREDIT_DBL") < 0))
    )

    helper_cols = ["SK_ID_CURR_INT", "SK_ID_PREV_INT", "AMT_APPLICATION_DBL", "AMT_CREDIT_DBL"]

    rejects_df = df_casted.filter(reject_filter).withColumn("reject_reason", reject_reason_expr).drop(*helper_cols)
    valid_df   = df_casted.filter(~reject_filter).drop(*helper_cols)

    return valid_df, rejects_df


def validate_pos_cash_balance(df: DataFrame):
    """
    Checks:
      - SK_ID_CURR : not null, castable to int
      - SK_ID_PREV : not null, castable to int
      - MONTHS_BALANCE: not null, castable to int
      - SK_DPD     : castable to double if present, >= 0
    """
    df_casted = (
        df
        .withColumn("SK_ID_CURR_INT",      col("SK_ID_CURR").cast("int"))
        .withColumn("SK_ID_PREV_INT",       col("SK_ID_PREV").cast("int"))
        .withColumn("MONTHS_BALANCE_INT",   col("MONTHS_BALANCE").cast("int"))
        .withColumn("SK_DPD_DBL",           col("SK_DPD").cast("double"))
    )

    reject_reason_expr = (
        when(col("SK_ID_CURR").isNull(),                                    lit("NULL_SK_ID_CURR"))
        .when(col("SK_ID_CURR_INT").isNull(),                               lit("INVALID_SK_ID_CURR_TYPE"))
        .when(col("SK_ID_PREV").isNull(),                                   lit("NULL_SK_ID_PREV"))
        .when(col("SK_ID_PREV_INT").isNull(),                               lit("INVALID_SK_ID_PREV_TYPE"))
        .when(col("MONTHS_BALANCE").isNull(),                               lit("NULL_MONTHS_BALANCE"))
        .when(col("MONTHS_BALANCE_INT").isNull(),                           lit("INVALID_MONTHS_BALANCE_TYPE"))
        .when(col("SK_DPD").isNotNull() & col("SK_DPD_DBL").isNull(),       lit("INVALID_SK_DPD_TYPE"))
        .when(col("SK_DPD_DBL").isNotNull() & (col("SK_DPD_DBL") < 0),     lit("NEGATIVE_SK_DPD"))
    )

    reject_filter = (
        col("SK_ID_CURR").isNull() |
        col("SK_ID_CURR_INT").isNull() |
        col("SK_ID_PREV").isNull() |
        col("SK_ID_PREV_INT").isNull() |
        col("MONTHS_BALANCE").isNull() |
        col("MONTHS_BALANCE_INT").isNull() |
        (col("SK_DPD").isNotNull() & col("SK_DPD_DBL").isNull()) |
        (col("SK_DPD_DBL").isNotNull() & (col("SK_DPD_DBL") < 0))
    )

    helper_cols = ["SK_ID_CURR_INT", "SK_ID_PREV_INT", "MONTHS_BALANCE_INT", "SK_DPD_DBL"]

    rejects_df = df_casted.filter(reject_filter).withColumn("reject_reason", reject_reason_expr).drop(*helper_cols)
    valid_df   = df_casted.filter(~reject_filter).drop(*helper_cols)

    return valid_df, rejects_df


def validate_installments_payments(df: DataFrame):
    """
    Checks:
      - SK_ID_CURR  : not null, castable to int
      - SK_ID_PREV  : not null, castable to int
      - AMT_PAYMENT : castable to double if present, >= 0
      - AMT_INSTALMENT: castable to double if present, >= 0
    """
    df_casted = (
        df
        .withColumn("SK_ID_CURR_INT",       col("SK_ID_CURR").cast("int"))
        .withColumn("SK_ID_PREV_INT",        col("SK_ID_PREV").cast("int"))
        .withColumn("AMT_PAYMENT_DBL",       col("AMT_PAYMENT").cast("double"))
        .withColumn("AMT_INSTALMENT_DBL",    col("AMT_INSTALMENT").cast("double"))
    )

    reject_reason_expr = (
        when(col("SK_ID_CURR").isNull(),                                              lit("NULL_SK_ID_CURR"))
        .when(col("SK_ID_CURR_INT").isNull(),                                         lit("INVALID_SK_ID_CURR_TYPE"))
        .when(col("SK_ID_PREV").isNull(),                                             lit("NULL_SK_ID_PREV"))
        .when(col("SK_ID_PREV_INT").isNull(),                                         lit("INVALID_SK_ID_PREV_TYPE"))
        .when(col("AMT_PAYMENT").isNotNull() & col("AMT_PAYMENT_DBL").isNull(),       lit("INVALID_AMT_PAYMENT_TYPE"))
        .when(col("AMT_PAYMENT_DBL").isNotNull() & (col("AMT_PAYMENT_DBL") < 0),      lit("NEGATIVE_AMT_PAYMENT"))
        .when(col("AMT_INSTALMENT").isNotNull() & col("AMT_INSTALMENT_DBL").isNull(), lit("INVALID_AMT_INSTALMENT_TYPE"))
        .when(col("AMT_INSTALMENT_DBL").isNotNull() & (col("AMT_INSTALMENT_DBL") < 0), lit("NEGATIVE_AMT_INSTALMENT"))
    )

    reject_filter = (
        col("SK_ID_CURR").isNull() |
        col("SK_ID_CURR_INT").isNull() |
        col("SK_ID_PREV").isNull() |
        col("SK_ID_PREV_INT").isNull() |
        (col("AMT_PAYMENT").isNotNull() & col("AMT_PAYMENT_DBL").isNull()) |
        (col("AMT_PAYMENT_DBL").isNotNull() & (col("AMT_PAYMENT_DBL") < 0)) |
        (col("AMT_INSTALMENT").isNotNull() & col("AMT_INSTALMENT_DBL").isNull()) |
        (col("AMT_INSTALMENT_DBL").isNotNull() & (col("AMT_INSTALMENT_DBL") < 0))
    )

    helper_cols = ["SK_ID_CURR_INT", "SK_ID_PREV_INT", "AMT_PAYMENT_DBL", "AMT_INSTALMENT_DBL"]

    rejects_df = df_casted.filter(reject_filter).withColumn("reject_reason", reject_reason_expr).drop(*helper_cols)
    valid_df   = df_casted.filter(~reject_filter).drop(*helper_cols)

    return valid_df, rejects_df


def validate_credit_card_balance(df: DataFrame):
    """
    Checks:
      - SK_ID_CURR  : not null, castable to int
      - SK_ID_PREV  : not null, castable to int
      - MONTHS_BALANCE: not null, castable to int
      - AMT_BALANCE : castable to double if present
    """
    df_casted = (
        df
        .withColumn("SK_ID_CURR_INT",      col("SK_ID_CURR").cast("int"))
        .withColumn("SK_ID_PREV_INT",       col("SK_ID_PREV").cast("int"))
        .withColumn("MONTHS_BALANCE_INT",   col("MONTHS_BALANCE").cast("int"))
        .withColumn("AMT_BALANCE_DBL",      col("AMT_BALANCE").cast("double"))
    )

    reject_reason_expr = (
        when(col("SK_ID_CURR").isNull(),                                      lit("NULL_SK_ID_CURR"))
        .when(col("SK_ID_CURR_INT").isNull(),                                 lit("INVALID_SK_ID_CURR_TYPE"))
        .when(col("SK_ID_PREV").isNull(),                                     lit("NULL_SK_ID_PREV"))
        .when(col("SK_ID_PREV_INT").isNull(),                                 lit("INVALID_SK_ID_PREV_TYPE"))
        .when(col("MONTHS_BALANCE").isNull(),                                 lit("NULL_MONTHS_BALANCE"))
        .when(col("MONTHS_BALANCE_INT").isNull(),                             lit("INVALID_MONTHS_BALANCE_TYPE"))
        .when(col("AMT_BALANCE").isNotNull() & col("AMT_BALANCE_DBL").isNull(), lit("INVALID_AMT_BALANCE_TYPE"))
    )

    reject_filter = (
        col("SK_ID_CURR").isNull() |
        col("SK_ID_CURR_INT").isNull() |
        col("SK_ID_PREV").isNull() |
        col("SK_ID_PREV_INT").isNull() |
        col("MONTHS_BALANCE").isNull() |
        col("MONTHS_BALANCE_INT").isNull() |
        (col("AMT_BALANCE").isNotNull() & col("AMT_BALANCE_DBL").isNull())
    )

    helper_cols = ["SK_ID_CURR_INT", "SK_ID_PREV_INT", "MONTHS_BALANCE_INT", "AMT_BALANCE_DBL"]

    rejects_df = df_casted.filter(reject_filter).withColumn("reject_reason", reject_reason_expr).drop(*helper_cols)
    valid_df   = df_casted.filter(~reject_filter).drop(*helper_cols)

    return valid_df, rejects_df


# ---------------------------------------------------------------------------
# Dispatch map
# ---------------------------------------------------------------------------
VALIDATORS = {
    "application_train":    validate_application_train,
    "bureau":               validate_bureau,
    "bureau_balance":       validate_bureau_balance,
    "previous_application": validate_previous_application,
    "pos_cash_balance":     validate_pos_cash_balance,
    "installments_payments":validate_installments_payments,
    "credit_card_balance":  validate_credit_card_balance,
}


# ---------------------------------------------------------------------------
# JDBC writer
# ---------------------------------------------------------------------------
def write_jdbc(df: DataFrame, table_name: str, mode: str = "overwrite"):
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
# Main
# ---------------------------------------------------------------------------
def main():
    builder = (
        SparkSession.builder
        .appName("de3-dq-validate")
        .config("spark.sql.adaptive.enabled", "true")
    )

    spark_master = os.getenv("SPARK_MASTER")
    if spark_master:
        builder = builder.master(spark_master)

    spark = builder.getOrCreate()

    for table_name in TABLES:
        raw_table        = f"raw.{table_name}"
        silver_table     = f"silver.{table_name}"
        quarantine_table = f"quarantine.{table_name}_rejects"

        print(f"\n=== Validating: {raw_table}")

        # Read from raw
        df = spark.read.jdbc(
            url=jdbc_url(),
            table=raw_table,
            properties=jdbc_properties(),
        )

        # Run table-specific DQ validator
        validator = VALIDATORS[table_name]
        valid_df, rejects_df = validator(df)

        valid_count  = valid_df.count()
        reject_count = rejects_df.count()

        print(f"  valid={valid_count} | rejects={reject_count} | total={valid_count + reject_count}")

        # Write valid rows → silver
        print(f"  Writing valid rows → {silver_table}")
        write_jdbc(valid_df.coalesce(2), silver_table, mode="overwrite")

        # Write rejects → quarantine (only if any)
        if reject_count > 0:
            print(f"  Writing {reject_count} rejected rows → {quarantine_table}")
            write_jdbc(rejects_df.coalesce(1), quarantine_table, mode="overwrite")
        else:
            print(f"  No rejects for {table_name} ✅")

    spark.stop()
    print("\n✅ DQ validation complete — clean data in silver, bad rows in quarantine.")


if __name__ == "__main__":
    main()