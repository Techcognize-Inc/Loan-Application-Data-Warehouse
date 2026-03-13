import great_expectations as gx

def add_asset(datasource):
    asset = datasource.add_query_asset(
        name="fact_loans",
        query="SELECT * FROM warehouse_warehouse.fact_loans LIMIT 10000",
    )
    return asset.add_batch_definition_whole_table("loans_batch")

def build_suite(context):
    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="fact_loans_suite")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_key")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="avg_credit_amount", min_value=0
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="avg_income", min_value=0
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="loan_application_count", min_value=1
        )
    )

    return suite
