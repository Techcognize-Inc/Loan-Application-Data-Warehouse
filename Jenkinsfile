pipeline {
    agent any

    stages {
        stage('Validate Project Files') {
            steps {
                sh 'test -f scripts/run_ingest.sh'
                sh 'test -f scripts/run_staging.sh'
                sh 'test -f dags/loan_warehouse_dag.py'
                sh 'test -f great_expectations_suite/run_validations.py'
                sh 'echo "Required project files exist."'
            }
        }

        stage('Trigger Airflow DAG') {
            steps {
                sh """
                docker exec de3-airflow-webserver airflow dags trigger loan_warehouse_dag
                """
            }
        }

        stage('Success') {
            steps {
                echo 'Jenkins pipeline completed successfully and Airflow DAG was triggered.'
            }
        }
    }
}