
# Loan Application Data Warehouse

This project builds a complete **data engineering pipeline** for loan application analytics using **Spark, Airflow, dbt, and PostgreSQL**.

The pipeline ingests raw CSV data, transforms it using Spark, builds warehouse models using dbt, and orchestrates everything using Airflow.

---

# Architecture

Raw Data → Spark Ingestion → Raw Schema → Spark Transformations → Staging Schema → dbt Models → Warehouse Schema → Data Quality Checks → Airflow Orchestration

Technologies used:

- Apache Spark
- Apache Airflow
- PostgreSQL
- dbt
- Great Expectations
- Docker
- Python

---

# Project Structure

Loan-Application-Data-Warehouse
│
├── data
│   └── raw
│
├── spark_jobs
│   ├── ingest_raw.py
│   ├── build_staging.py
│   └── common
│
├── scripts
│   ├── run_ingest.sh
│   └── run_staging.sh
│
├── dags
│   └── loan_warehouse_dag.py
│
├── dbt
│   └── models
│
├── great_expectations_suite
│
├── docker
│   └── docker-compose.yml
│
└── README.md

---

# Data Pipeline

The pipeline follows a **multi-layer warehouse architecture**.

### Raw Layer
Raw data is ingested using Spark from CSV files into PostgreSQL.

Tables:

- raw.application_train
- raw.bureau
- raw.previous_application
- raw.pos_cash_balance
- raw.installments_payments
- raw.credit_card_balance

---

### Staging Layer

Spark transformations create a cleaned dataset:

staging.stg_loan_application_enriched

Features are aggregated from multiple source tables and joined with application data.

Each row represents **one loan application (SK_ID_CURR)**.

---

### Warehouse Layer

dbt builds analytical warehouse models.

Tables:

warehouse.dim_customer
warehouse.fact_loans
warehouse.dim_bureau

---

# Data Quality Checks

Two layers of data validation are used.

### dbt tests

Checks:

- primary key uniqueness
- not null constraints
- relationship tests

### Great Expectations

Checks:

- row counts
- null validations
- schema validation

---

# Orchestration (Airflow)

Airflow orchestrates the entire pipeline.

DAG Flow:

ingest_raw
↓
build_staging
↓
run_validations
↓
dbt_run
↓
dbt_test

Each task runs automatically using Airflow scheduling.

---

# Running the Project

### Start Docker services

cd docker
docker compose up -d

---

### Run raw ingestion

bash scripts/run_ingest.sh

---

### Run staging transformation

bash scripts/run_staging.sh

---

### Run dbt models

dbt run –profiles-dir airflow/.dbt

---

### Run data quality tests

dbt test –profiles-dir airflow/.dbt

---

### Trigger Airflow DAG

Open:

http://localhost:8082

Trigger DAG:

loan_warehouse_dag

---

# Final Row Counts

| Table | Row Count |
|-----|-----|
| raw.application_train | 307511 |
| staging.stg_loan_application_enriched | 307511 |
| warehouse.dim_customer | 307511 |
| warehouse.fact_loans | 307511 |
| warehouse.dim_bureau | 1716428 |

---

# Key Features

- Distributed data processing using **Apache Spark**
- Scalable warehouse modeling using **dbt**
- Automated orchestration using **Apache Airflow**
- Data validation using **Great Expectations**
- Fully containerized environment using **Docker**

---

# Future Improvements

- Incremental data loading
- Data lineage tracking
- Monitoring and alerting
- CI/CD pipeline integration
