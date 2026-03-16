pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                echo 'Using checked out project code'
            }
        }

        stage('Validate Project Files') {
            steps {
                sh 'test -f scripts/run_ingest.sh'
                sh 'test -f scripts/run_staging.sh'
                sh 'test -f dags/loan_warehouse_dag.py'
                sh 'test -f great_expectations_suite/run_validations.py'
                sh 'echo "Required project files exist."'
            }
        }

        stage('Run dbt Tests') {
            steps {
                sh 'docker exec de3-airflow-webserver bash -c "cd /opt/airflow/project && dbt test --profiles-dir /opt/airflow/.dbt"'
            }
        }

        stage('Run Great Expectations') {
            steps {
                sh 'docker exec de3-airflow-webserver bash -c "cd /opt/airflow/project && python great_expectations_suite/run_validations.py"'
            }
        }

        stage('Success') {
            steps {
                echo 'Jenkins pipeline completed successfully.'
            }
        }
    }
}