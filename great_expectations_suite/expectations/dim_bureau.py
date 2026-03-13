import great_expectations as gx

def add_asset(datasource):
    asset = datasource.add_table_asset(
        name="dim_bureau",
        table_name="dim_bureau",
        schema_name="warehouse_warehouse",
    )
    return asset.add_batch_definition_whole_table("bureau_batch")

def build_suite(context):
    suite = context.suites.add_or_update(
        gx.ExpectationSuite(name="dim_bureau_suite")
    )

    # Row count only — fastest possible check
    suite.add_expectation(
        gx.expectations.ExpectTableRowCountToBeBetween(
            min_value=1700000, max_value=1750000
        )
    )

    return suite
