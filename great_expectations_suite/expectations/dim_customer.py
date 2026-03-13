import great_expectations as gx

def add_asset(datasource):
    asset = datasource.add_query_asset(
        name="dim_customer",
        query="SELECT * FROM warehouse_warehouse.dim_customer LIMIT 10000",
    )
    return asset.add_batch_definition_whole_table("customer_batch")

def build_suite(context):
    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="dim_customer_suite")
    )

    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(column="customer_key")
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="code_gender", value_set=["M", "F", "XNA"]
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="amt_income_total", min_value=0
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="amt_credit", min_value=0
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="target", value_set=[0, 1]
        )
    )

    return suite
