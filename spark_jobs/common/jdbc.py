def jdbc_url():
    return "jdbc:postgresql://localhost:5432/analytics_dev"

def jdbc_properties():
    return {
        "user": "revanth",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }