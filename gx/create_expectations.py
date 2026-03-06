import great_expectations as gx

context = gx.get_context()

datasource = context.get_datasource("postgres_db")

asset = datasource.get_asset("loan_data_asset")

batch_request = asset.build_batch_request()

suite_name = "loan_data_suite"

# Create expectation suite if it doesn't exist
try:
    context.get_expectation_suite(suite_name)
except:
    context.add_expectation_suite(expectation_suite_name=suite_name)

validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=suite_name,
)

# Add expectations
validator.expect_column_to_exist("loan_id")
validator.expect_column_values_to_not_be_null("loan_id")
validator.expect_column_values_to_be_between("loan_amount", min_value=0)

validator.save_expectation_suite()

print("Expectations created successfully!")
