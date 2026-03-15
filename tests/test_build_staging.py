from pyspark.sql.functions import col, count, avg, max as fmax, sum as fsum


def test_bureau_aggregation_columns(spark):
    bureau = spark.createDataFrame(
        [
            (100001, 1, 1000.0, 0),
            (100001, 2, 2000.0, 10),
            (100002, 3, 500.0, 0),
        ],
        ["SK_ID_CURR", "SK_ID_BUREAU", "AMT_CREDIT_SUM", "CREDIT_DAY_OVERDUE"],
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

    cols = set(bureau_agg.columns)
    assert "SK_ID_CURR" in cols
    assert "bureau_records_cnt" in cols
    assert "bureau_avg_credit_sum" in cols
    assert "bureau_max_credit_sum" in cols
    assert "bureau_avg_days_overdue" in cols
    assert "bureau_max_days_overdue" in cols


def test_staging_join_preserves_one_row_per_customer(spark):
    # Base app table (grain should be 1 row per SK_ID_CURR)
    app = spark.createDataFrame(
        [
            (100001, 1, 250000.0),
            (100002, 0, 120000.0),
        ],
        ["SK_ID_CURR", "TARGET", "AMT_CREDIT"],
    )

    # Child tables with multiple rows per SK_ID_CURR
    prev = spark.createDataFrame(
        [
            (100001, 10, 50000.0),
            (100001, 11, 60000.0),
            (100002, 12, 40000.0),
        ],
        ["SK_ID_CURR", "SK_ID_PREV", "AMT_APPLICATION"],
    )

    inst = spark.createDataFrame(
        [
            (100001, 10, 1000.0),
            (100001, 10, 2000.0),
            (100002, 12, 500.0),
        ],
        ["SK_ID_CURR", "SK_ID_PREV", "AMT_PAYMENT"],
    )

    # Aggregate children to 1 row per SK_ID_CURR (same idea as build_staging.py)
    prev_agg = (
        prev.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("prev_app_cnt"),
            avg(col("AMT_APPLICATION")).alias("prev_avg_amt_application"),
        )
    )

    inst_agg = (
        inst.groupBy("SK_ID_CURR")
        .agg(
            count("*").alias("inst_records_cnt"),
            fsum(col("AMT_PAYMENT")).alias("inst_total_amt_paid"),
        )
    )

    stg = (
        app.join(prev_agg, on="SK_ID_CURR", how="left")
           .join(inst_agg, on="SK_ID_CURR", how="left")
    )

    # Grain check: still 1 row per SK_ID_CURR
    total = stg.count()
    distinct_keys = stg.select("SK_ID_CURR").distinct().count()
    assert total == distinct_keys == 2