from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "sairam",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=30),
}

with DAG(
    dag_id="loan_warehouse_dag",
    description="End-to-end Loan Application Data Warehouse pipeline (raw -> DQ -> silver -> staging -> dbt -> GE -> Iceberg)",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["ladw", "spark", "dbt", "ge", "dq", "iceberg"],
) as dag:

    # 1) Ingest raw CSVs -> Postgres raw.*
    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command='bash -c "cd /opt/airflow/project && bash scripts/run_ingest.sh"',
    )

    # 2) Data Quality validation: raw.* -> silver.* + quarantine.*
    dq_validate = BashOperator(
        task_id="dq_validate",
        bash_command='bash -c "cd /opt/airflow/project && bash scripts/run_dq.sh"',
    )

    # 3) Build staging from CLEAN tables (silver.*) -> staging.stg_loan_application_enriched
    build_staging = BashOperator(
        task_id="build_staging",
        bash_command='bash -c "cd /opt/airflow/project && bash scripts/run_staging.sh"',
    )

    # 4) Optional staging validations (if your script exists and you still want it)
    validate_staging = BashOperator(
        task_id="validate_staging",
        bash_command='bash -c "cd /opt/airflow/project && bash scripts/run_staging_validations.sh"',
    )

    # 5) dbt transforms -> warehouse.*
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command='bash -c "cd /opt/airflow/project && dbt run --profiles-dir /opt/airflow/.dbt"',
    )

    # 6) Great Expectations validations (typically on staging/warehouse)
    run_validations = BashOperator(
        task_id="run_validations",
        bash_command='bash -c "cd /opt/airflow/project && python great_expectations_suite/run_validations.py"',
    )

    # 7) dbt tests
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command='bash -c "cd /opt/airflow/project && dbt test --profiles-dir /opt/airflow/.dbt"',
    )

    # 8) Land warehouse tables -> Iceberg (Hadoop catalog local warehouse)
    land_warehouse_to_iceberg = BashOperator(
        task_id="land_warehouse_to_iceberg",
        bash_command='bash -c "cd /opt/airflow/project && bash scripts/run_land_warehouse_to_iceberg.sh"',
    )

    ingest_raw >> dq_validate >> build_staging >> validate_staging >> dbt_run >> run_validations >> dbt_test >> land_warehouse_to_iceberg