pipeline {
    agent any

    environment {
        VENV_PATH        = "${WORKSPACE}/venv"
        DBT_PROFILES_DIR = "${WORKSPACE}/.dbt"
        DB_HOST          = "jenkins-postgres"
        DB_PORT          = "5432"
        DB_NAME          = "analytics_dev"
        DB_USER          = "revanth"
        DB_PASSWORD      = credentials('postgres-password')
        DOCKER_IMAGE     = "loan-dw"
        DOCKER_TAG       = "${BUILD_NUMBER}"
        AIRFLOW_URL      = "http://192.168.1.215:9090"
        AIRFLOW_USER     = "admin"
        AIRFLOW_PASSWORD = credentials('airflow-password')
    }

    triggers {
        cron('0 2 * * *')
        pollSCM('H/5 * * * *')
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
                echo "✅ Checked out branch: ${env.BRANCH_NAME}"
            }
        }

        stage('Setup Python') {
            steps {
                sh '''
                    python3 -m venv ${VENV_PATH}
                    ${VENV_PATH}/bin/pip install --upgrade pip
                    ${VENV_PATH}/bin/pip install \
                        dbt-postgres \
                        great-expectations \
                        psycopg2-binary \
                        pandas \
                        sqlalchemy \
                        pyspark \
                        pytest
                    ${VENV_PATH}/bin/pip install -e .
                '''
            }
        }

        stage('Run Pytest') {
            steps {
                sh '${VENV_PATH}/bin/pytest --tb=short -q'
            }
            post {
                failure {
                    echo "❌ Pytest failed — aborting pipeline"
                }
            }
        }

        stage('Setup dbt Profiles') {
            steps {
                sh '''
                    mkdir -p ${DBT_PROFILES_DIR}
                    cat > ${DBT_PROFILES_DIR}/profiles.yml << PROFILE
loan_dw:
  target: dev
  outputs:
    dev:
      type: postgres
      host: ${DB_HOST}
      port: ${DB_PORT}
      user: ${DB_USER}
      password: ${DB_PASSWORD}
      dbname: ${DB_NAME}
      schema: public
      threads: 4
PROFILE
                '''
            }
        }

        stage('Seed Database') {
            steps {
                sh '''
                    PGPASSWORD=${DB_PASSWORD} psql -h ${DB_HOST} -U ${DB_USER} -d ${DB_NAME} << SQL
                    CREATE SCHEMA IF NOT EXISTS raw;
                    CREATE SCHEMA IF NOT EXISTS staging;
                    CREATE SCHEMA IF NOT EXISTS warehouse;

                    CREATE TABLE IF NOT EXISTS raw.application_train (
                        "SK_ID_CURR"          INT,
                        "TARGET"              INT,
                        "CODE_GENDER"         VARCHAR(10),
                        "AMT_INCOME_TOTAL"    FLOAT,
                        "AMT_CREDIT"          FLOAT,
                        "AMT_ANNUITY"         FLOAT,
                        "NAME_EDUCATION_TYPE" VARCHAR(50),
                        "OCCUPATION_TYPE"     VARCHAR(50),
                        "ORGANIZATION_TYPE"   VARCHAR(50)
                    );

                    CREATE TABLE IF NOT EXISTS raw.bureau (
                        "SK_ID_BUREAU"           INT,
                        "SK_ID_CURR"             INT,
                        "CREDIT_ACTIVE"          VARCHAR(20),
                        "CREDIT_CURRENCY"        VARCHAR(20),
                        "DAYS_CREDIT"            INT,
                        "CREDIT_DAY_OVERDUE"     INT,
                        "DAYS_CREDIT_ENDDATE"    FLOAT,
                        "DAYS_ENDDATE_FACT"      FLOAT,
                        "AMT_CREDIT_MAX_OVERDUE" FLOAT,
                        "CNT_CREDIT_PROLONG"     INT,
                        "AMT_CREDIT_SUM"         FLOAT,
                        "AMT_CREDIT_SUM_DEBT"    FLOAT,
                        "AMT_CREDIT_SUM_LIMIT"   FLOAT,
                        "AMT_CREDIT_SUM_OVERDUE" FLOAT,
                        "CREDIT_TYPE"            VARCHAR(50),
                        "DAYS_CREDIT_UPDATE"     INT,
                        "AMT_ANNUITY"            FLOAT
                    );

                    INSERT INTO raw.application_train
                    SELECT * FROM (VALUES
                        (1, 0, 'M',   50000, 100000, 5000, 'Secondary',        'Laborers',    'Business Entity Type 3'),
                        (2, 1, 'F',   30000,  80000, 4000, 'Higher education', 'Sales staff', 'School'),
                        (3, 0, 'XNA', 45000, 120000, 6000, 'Secondary',        NULL,          'Government')
                    ) AS v WHERE NOT EXISTS (SELECT 1 FROM raw.application_train);

                    INSERT INTO raw.bureau
                    SELECT * FROM (VALUES
                        (1001, 1, 'Active', 'currency 1', -365, 0,    0::FLOAT, NULL::FLOAT,  0::FLOAT, 0, 50000::FLOAT, 10000::FLOAT, 40000::FLOAT, 0::FLOAT, 'Consumer credit', -100, 1000::FLOAT),
                        (1002, 2, 'Closed', 'currency 2', -730, 0, -365::FLOAT, -300::FLOAT,  0::FLOAT, 1, 30000::FLOAT,     0::FLOAT, 30000::FLOAT, 0::FLOAT, 'Credit card',     -200,  500::FLOAT)
                    ) AS v WHERE NOT EXISTS (SELECT 1 FROM raw.bureau);
SQL
                '''
            }
        }

        stage('dbt - Install Packages') {
            steps {
                sh '${VENV_PATH}/bin/dbt deps --profiles-dir ${DBT_PROFILES_DIR}'
            }
        }

        stage('dbt - Run Models') {
            steps {
                sh '${VENV_PATH}/bin/dbt run --profiles-dir ${DBT_PROFILES_DIR}'
            }
            post {
                failure {
                    echo "❌ dbt run failed"
                }
            }
        }

        stage('dbt - Test Models') {
            steps {
                sh '${VENV_PATH}/bin/dbt test --profiles-dir ${DBT_PROFILES_DIR}'
            }
            post {
                failure {
                    echo "❌ dbt tests failed"
                }
            }
        }

        stage('Great Expectations') {
            steps {
                sh '''
                    export DB_HOST=${DB_HOST}
                    export DB_PASSWORD=${DB_PASSWORD}
                    ${VENV_PATH}/bin/python great_expectations_suite/run_validations.py
                '''
            }
            post {
                failure {
                    echo "❌ Great Expectations validation failed"
                }
            }
        }

        stage('Build Docker Image') {
            when {
                anyOf {
                    branch 'main'
                    triggeredBy 'UserIdCause'
                }
            }
            steps {
                sh '''
                    docker build -t ${DOCKER_IMAGE}:${DOCKER_TAG} .
                    docker tag ${DOCKER_IMAGE}:${DOCKER_TAG} ${DOCKER_IMAGE}:latest
                    echo "✅ Docker image built: ${DOCKER_IMAGE}:${DOCKER_TAG}"
                '''
            }
        }

        stage('Trigger Airflow DAG') {
            when {
                anyOf {
                    branch 'main'
                    triggeredBy 'UserIdCause'
                }
            }
            steps {
                script {
                    def airflowUser = env.AIRFLOW_USER
                    def airflowPass = env.AIRFLOW_PASSWORD
                    def airflowUrl  = env.AIRFLOW_URL
                    def buildNum    = env.BUILD_NUMBER

                    sh """
                        echo '🔑 Getting Airflow token...'
                        python3 -c \"
import urllib.request, json, sys

# Get token
token_data = json.dumps({'username': '${airflowUser}', 'password': '${airflowPass}'}).encode()
token_req = urllib.request.Request(
    '${airflowUrl}/auth/token',
    data=token_data,
    headers={'Content-Type': 'application/json'},
    method='POST'
)
with urllib.request.urlopen(token_req) as r:
    token = json.loads(r.read())['access_token']

print('✅ Got token')

# Trigger DAG
dag_data = json.dumps({'logical_date': None, 'conf': {}, 'note': 'Triggered by Jenkins build #${buildNum}'}).encode()
dag_req = urllib.request.Request(
    '${airflowUrl}/api/v2/dags/loan_warehouse_pipeline/dagRuns',
    data=dag_data,
    headers={'Content-Type': 'application/json', 'Authorization': 'Bearer ' + token},
    method='POST'
)
with urllib.request.urlopen(dag_req) as r:
    result = json.loads(r.read())
    print('✅ Airflow DAG triggered:', result['dag_run_id'])
\"
                    """
                }
            }
            post {
                failure {
                    echo "❌ Failed to trigger Airflow DAG"
                }
            }
        }

        stage('Deploy') {
            when {
                allOf {
                    branch 'main'
                    not { triggeredBy 'TimerTrigger' }
                }
            }
            steps {
                input message: 'Deploy to production?', ok: 'Deploy'
                sh '''
                    echo "🚀 Deploying ${DOCKER_IMAGE}:${DOCKER_TAG}..."
                    docker stop loan-dw-app || true
                    docker rm loan-dw-app || true
                    docker run -d \
                        --name loan-dw-app \
                        --network jenkins-network \
                        --env DB_HOST=${DB_HOST} \
                        --env DB_NAME=${DB_NAME} \
                        --env DB_USER=${DB_USER} \
                        --env DB_PASSWORD=${DB_PASSWORD} \
                        ${DOCKER_IMAGE}:${DOCKER_TAG}
                    echo "✅ Deployed successfully"
                '''
            }
        }
    }

    post {
        success {
            echo "🎉 Pipeline completed successfully!"
        }
        failure {
            echo "💥 Pipeline failed — check logs above"
        }
        always {
            cleanWs()
        }
    }
}
