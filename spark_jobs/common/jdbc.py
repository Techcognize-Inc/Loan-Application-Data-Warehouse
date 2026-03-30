# spark_jobs/common/jdbc.py
import os

def jdbc_url() -> str:
    host = os.getenv("DB_HOST", "postgres")
    port = os.getenv("DB_PORT", "5432")
    db   = os.getenv("DB_NAME", "bankingdb")
    return f"jdbc:postgresql://{host}:{port}/{db}"

def jdbc_properties() -> dict:
    return {
        "user": os.getenv("DB_USER", "postgres"),
        "password": os.getenv("DB_PASSWORD", "password"),
        "driver": "org.postgresql.Driver",
    }