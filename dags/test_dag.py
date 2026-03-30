from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="test_dag",
    start_date=datetime(2026, 3, 1),
    schedule=None,
    catchup=False,
    tags=["test"],
) as dag:

    hello = BashOperator(
        task_id="hello",
        bash_command="echo 'Airflow is working'"
    )