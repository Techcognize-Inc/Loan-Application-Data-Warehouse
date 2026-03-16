"""
Great Expectations Validation Runner
=====================================
Uses GX with Pandas engine for fast in-memory validation.
Fetches sample data via psycopg2, validates with GX.

Usage:
    python great_expectations_suite/run_validations.py
"""

import great_expectations as gx
from great_expectations.datasource.fluent import PandasDatasource
import pandas as pd
import psycopg2
import sys
import os

DB_PARAMS = {
    "host":     os.getenv("DB_HOST", "localhost"),
    "port":     int(os.getenv("DB_PORT", 5433)),
    "dbname":   os.getenv("DB_NAME", "bankingdb"),
    "user":     os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "password"),
}

def fetch(query):
    conn = psycopg2.connect(**DB_PARAMS)
    df = pd.read_sql(query, conn)
    conn.close()
    return df

def run():
    print("\n🚀 Starting Great Expectations Validations")
    print("=" * 50)

    # ── Fetch data ────────────────────────────────
    print("\n📥 Fetching sample data...")
    df_customer = fetch("SELECT * FROM warehouse.dim_customer LIMIT 10000")
    df_loans    = fetch("SELECT * FROM warehouse.fact_loans LIMIT 10000")
    df_bureau   = fetch("SELECT bureau_id FROM warehouse.dim_bureau LIMIT 10000")
    print("✅ Data fetched!")

    # ── GX context with Pandas ────────────────────
    context = gx.get_context(mode="ephemeral")
    ds = context.data_sources.add_pandas(name="loan_warehouse")

    # ── dim_customer ──────────────────────────────
    customer_asset = ds.add_dataframe_asset(name="dim_customer")
    customer_batch = customer_asset.add_batch_definition_whole_dataframe("customer_batch")
    customer_suite = context.suites.add_or_update(gx.ExpectationSuite(name="dim_customer_suite"))
    customer_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_key"))
    customer_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="code_gender", value_set=["M", "F", "XNA"]))
    customer_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amt_income_total", min_value=0))
    customer_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="amt_credit", min_value=0))
    customer_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeInSet(column="target", value_set=[0, 1]))
    customer_val = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name="validate_dim_customer", data=customer_batch, suite=customer_suite)
    )

    # ── fact_loans ────────────────────────────────
    loans_asset = ds.add_dataframe_asset(name="fact_loans")
    loans_batch = loans_asset.add_batch_definition_whole_dataframe("loans_batch")
    loans_suite = context.suites.add_or_update(gx.ExpectationSuite(name="fact_loans_suite"))
    loans_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_key"))
    loans_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="avg_credit_amount", min_value=0))
    loans_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="avg_income", min_value=0))
    loans_suite.add_expectation(gx.expectations.ExpectColumnValuesToBeBetween(column="loan_application_count", min_value=1))
    loans_val = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name="validate_fact_loans", data=loans_batch, suite=loans_suite)
    )

    # ── dim_bureau ────────────────────────────────
    bureau_asset = ds.add_dataframe_asset(name="dim_bureau")
    bureau_batch = bureau_asset.add_batch_definition_whole_dataframe("bureau_batch")
    bureau_suite = context.suites.add_or_update(gx.ExpectationSuite(name="dim_bureau_suite"))
    bureau_suite.add_expectation(gx.expectations.ExpectColumnValuesToNotBeNull(column="bureau_id"))
    bureau_val = context.validation_definitions.add_or_update(
        gx.ValidationDefinition(name="validate_dim_bureau", data=bureau_batch, suite=bureau_suite)
    )

    # ── Run validations ───────────────────────────
    print("\n🔍 Running validations...")
    r1 = customer_val.run(batch_parameters={"dataframe": df_customer})
    r2 = loans_val.run(batch_parameters={"dataframe": df_loans})
    r3 = bureau_val.run(batch_parameters={"dataframe": df_bureau})

    # ── Results ───────────────────────────────────
    print(f"\n{'=' * 50}")
    print("📋 VALIDATION RESULTS")
    print(f"{'=' * 50}")
    print(f"  dim_customer  → {'✅ PASSED' if r1.success else '❌ FAILED'}")
    print(f"  fact_loans    → {'✅ PASSED' if r2.success else '❌ FAILED'}")
    print(f"  dim_bureau    → {'✅ PASSED' if r3.success else '❌ FAILED'}")
    print(f"{'=' * 50}")

    if not (r1.success and r2.success and r3.success):
        print("\n⚠️  Some validations failed!")
        sys.exit(1)
    else:
        print("\n🎉 All validations passed!")

if __name__ == "__main__":
    run()
