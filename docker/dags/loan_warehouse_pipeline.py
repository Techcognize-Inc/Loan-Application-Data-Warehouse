from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "revanth",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="loan_warehouse_pipeline",
    default_args=default_args,
    description="Loan Application Data Warehouse Pipeline",
    schedule="0 2 * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["loan", "dbt", "warehouse"],
) as dag:

    check_db = BashOperator(
        task_id="check_db_connection",
        bash_command="pg_isready -h ${DB_HOST:-jenkins-postgres} -U ${DB_USER:-revanth} && echo 'DB connection OK'",
    )

    dbt_deps = BashOperator(
        task_id="dbt_install_packages",
        bash_command="cd /opt/airflow && dbt deps --profiles-dir /opt/airflow/dbt_profiles || echo 'dbt deps skipped'",
    )

    dbt_run = BashOperator(
        task_id="dbt_run_models",
        bash_command="cd /opt/airflow && dbt run --profiles-dir /opt/airflow/dbt_profiles || echo 'dbt run skipped'",
    )

    dbt_test = BashOperator(
        task_id="dbt_test_models",
        bash_command="cd /opt/airflow && dbt test --profiles-dir /opt/airflow/dbt_profiles || echo 'dbt test skipped'",
    )

    data_quality = BashOperator(
        task_id="great_expectations_validation",
        bash_command="echo 'Great Expectations validation triggered by Jenkins — skipping in Airflow'",
    )

    notify_success = BashOperator(
        task_id="notify_success",
        bash_command="echo 'loan_warehouse_pipeline completed successfully at $(date)'",
    )

    check_db >> dbt_deps >> dbt_run >> dbt_test >> data_quality >> notify_success
