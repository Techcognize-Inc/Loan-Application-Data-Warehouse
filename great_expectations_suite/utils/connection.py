import great_expectations as gx

def get_context():
    return gx.get_context(mode="ephemeral")

def get_datasource(context):
    return context.data_sources.add_or_update_postgres(
        name="loan_warehouse",
        connection_string="postgresql+psycopg2://postgres:password@localhost:5433/bankingdb",
    )
