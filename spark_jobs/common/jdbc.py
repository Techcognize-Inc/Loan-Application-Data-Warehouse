import os

def jdbc_url():
    host = os.getenv("DB_HOST", "localhost")
    port = os.getenv("DB_PORT", "5433")
    db   = os.getenv("DB_NAME", "bankingdb")
    return f"jdbc:postgresql://{host}:{port}/{db}"

def jdbc_properties():
    return {
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }