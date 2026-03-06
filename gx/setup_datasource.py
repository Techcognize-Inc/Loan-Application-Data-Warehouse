import great_expectations as gx

context = gx.get_context()

datasource = context.sources.add_postgres(
    name="postgres_db",
    connection_string="postgresql+psycopg2://revanth@localhost:5432/analytics_dev"
)

print("Datasource created successfully!")
