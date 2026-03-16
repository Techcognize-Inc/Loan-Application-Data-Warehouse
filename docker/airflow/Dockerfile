FROM apache/airflow:2.9.3

USER root
RUN apt-get update && apt-get install -y openjdk-17-jre-headless && apt-get clean

USER airflow
RUN pip install --no-cache-dir \
    pyspark==3.5.7 \
    dbt-core==1.10.20 \
    dbt-postgres==1.10.0 \
    great_expectations \
    psycopg2-binary \
    pandas