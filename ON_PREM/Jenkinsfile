pipeline {
  agent any

  environment {
    VENV = 'unit_testing_bd'
  }

  stages {
    stage('Checkout') {
      steps {
        checkout scm
      }
    }

    stage('Setup Python Environment') {
      steps {
        sh '''
          python3 -m venv ${VENV}
          source ${VENV}/bin/activate
          pip install --upgrade pip
          pip install -r requirements.txt
        '''
      }
    }

    stage('Run Unit Tests') {
      steps {
        sh '''
          source ${VENV}/bin/activate
          pytest --junitxml=pytest.xml
        '''
      }
    }

    stage('Publish Test Results') {
      steps {
        junit 'pytest.xml'
      }
    }
  }

  post {
    always {
      echo 'Cleaning up workspace...'
      deleteDir()
    }
    success {
      echo 'All tests passed successfully!'
    }
    failure {
      echo 'One or more tests failed.'
    }
  }
}
