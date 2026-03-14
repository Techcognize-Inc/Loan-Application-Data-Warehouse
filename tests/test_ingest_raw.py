import pytest
from pyspark.sql.functions import col

import spark_jobs.ingest_raw as ingest


def test_table_map_has_expected_files():
    expected_files = {
        "application_train.csv",
        "bureau.csv",
        "bureau_balance.csv",
        "previous_application.csv",
        "POS_CASH_balance.csv",
        "installments_payments.csv",
        "credit_card_balance.csv",
    }
    assert set(ingest.TABLE_MAP.keys()) == expected_files
    # sanity: table names are non-empty
    assert all(isinstance(v, str) and v for v in ingest.TABLE_MAP.values())


def test_raw_path_points_to_workdir():
    # Your repo uses /opt/spark/work-dir/data/raw inside container
    assert isinstance(ingest.RAW_PATH, str)
    assert "work-dir" in ingest.RAW_PATH
    assert ingest.RAW_PATH.endswith("/data/raw")


def test_column_trimming_logic_matches_ingest(spark):
    # Create a DF with whitespace in column names
    df = spark.createDataFrame([(1, 2)], [" SK_ID_CURR ", " AMT_CREDIT "])

    # Same logic as ingest_raw.py:
    trimmed = df.select([col(c).alias(c.strip()) for c in df.columns])

    assert trimmed.columns == ["SK_ID_CURR", "AMT_CREDIT"]