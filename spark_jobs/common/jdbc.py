def jdbc_url():
    return"jdbc:postgresql://de3-postgres:5432/bankingdb"

def jdbc_properties():
    return {
        "user": "postgres",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }