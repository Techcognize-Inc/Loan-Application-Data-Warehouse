FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . .

# Install Python dependencies
RUN pip install --no-cache-dir \
    dbt-postgres \
    great-expectations \
    psycopg2-binary \
    pandas \
    sqlalchemy \
    pyspark \
    pytest && \
    pip install -e .

# Default command runs the full pipeline
CMD ["python", "great_expectations_suite/run_validations.py"]
