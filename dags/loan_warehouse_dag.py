from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "sairam",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="loan_warehouse_dag",
    description="End-to-end Loan Application Data Warehouse pipeline",
    default_args=default_args,
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["ladw", "spark", "dbt", "ge"],
) as dag:

    ingest_raw = BashOperator(
        task_id="ingest_raw",
        bash_command='bash -c "cd /opt/airflow/project && bash scripts/run_ingest.sh"',
    )

    build_staging = BashOperator(
        task_id="build_staging",
        bash_command='bash -c "cd /opt/airflow/project && bash scripts/run_staging.sh"',
    )

    run_validations = BashOperator(
        task_id="run_validations",
        bash_command='bash -c "cd /opt/airflow/project && python great_expectations_suite/run_validations.py"',
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command='bash -c "cd /opt/airflow/project && dbt run --profiles-dir /opt/airflow/.dbt"',
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command='bash -c "cd /opt/airflow/project && dbt test --profiles-dir /opt/airflow/.dbt"',
    )

    ingest_raw >> build_staging >> run_validations >> dbt_run >> dbt_test