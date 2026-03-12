import great_expectations as gx
import pandas as pd
from sqlalchemy import create_engine

# Create GE context on disk
context = gx.get_context(mode="file")

print("Great Expectations initialized successfully")

# PostgreSQL connection
engine = create_engine("postgresql://revanth:password@localhost:5432/analytics_dev")

# Load data from staging table
df = pd.read_sql("SELECT * FROM staging.application_train LIMIT 1000", engine)

# Create validator
validator = context.sources.pandas_default.read_dataframe(df)

# Expectations
validator.expect_column_to_exist("sk_id_curr")
validator.expect_column_values_to_not_be_null("target")
validator.expect_column_values_to_be_between("amt_credit", min_value=0)

# Save expectation suite
validator.save_expectation_suite(discard_failed_expectations=False)

print("Expectations created successfully")
