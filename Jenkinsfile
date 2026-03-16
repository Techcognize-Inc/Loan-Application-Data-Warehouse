pipeline {
    agent any

    environment {
        VENV_PATH        = "${WORKSPACE}/venv"
        DBT_PROFILES_DIR = "${WORKSPACE}/.dbt"
        DB_HOST          = "localhost"
        DB_PORT          = "5432"
        DB_NAME          = "analytics_dev"
        DB_USER          = "revanth"
        DB_PASSWORD      = credentials('postgres-password')
        DOCKER_IMAGE     = "loan-dw"
        DOCKER_TAG       = "${BUILD_NUMBER}"
    }

    triggers {
        // Nightly at 2am
        cron('0 2 * * *')
        // Poll SCM for push to main every 5 mins
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
                sh '''
                    ${VENV_PATH}/bin/pytest --tb=short -q
                '''
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: '**/test-results/*.xml'
                }
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
                        --env DB_HOST=${DB_HOST} \
                        --env DB_NAME=${DB_NAME} \
                        --env DB_USER=${DB_USER} \
                        --env DB_PASSWORD=${DB_PASSWORD} \
                        -p 8080:8080 \
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
