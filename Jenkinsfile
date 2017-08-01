pipeline {
    agent any
	environment {
        ENV = "dev"
        HBASE_CONNECTOR_DIR = "/$ENV/sbr-hbase-connector/lib"
    }
    stages {
        stage('Build') {
            steps {
                sh 'mvn clean compile'
            }
        }
        stage('Unit Test') {
            steps {
                sh 'mvn test'
            }
        }
        stage('Integration Test') {
            steps {
                sh 'mvn integration-test'
            }
        }
		stage('Deploy') {
            steps {
				sh 'mvn package assembly:single'
            }
        }
    }
    post {
        always {
            junit '**/target/*-reports/*.xml'
        }
    }
}

def copyToHBaseNode() {
    echo "Deploying to $ENV"
    sshagent(credentials: ["sbr-$ENV-ci-ssh-key"]) {
        withCredentials([string(credentialsId: "sbr-hbase-node", variable: 'HBASE_NODE')]) {
            sh '''
                    ssh sbr-$ENV-ci@$HBASE_NODE mkdir -p $HBASE_CONNECTOR_DIR
                    scp ${WORKSPACE}/target/*-distribution.jar sbr-dev-ci@$HOST:$HBASE_CONNECTOR_DIR
					echo "Successfully copied jar files to $HBASE_CONNECTOR_DIR directory on $HBASE_NODE"
                 '''
        }
    }
}
