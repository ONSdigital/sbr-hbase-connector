pipeline {
    agent any
	environment {
        ENV = "dev"
        HBASE_CONNECTOR_DIR = "$ENV/sbr-hbase-connector"
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
	stage('Package') {
            steps {
		echo 'mvn package'
            }
        }
	stage('Deploy') {
            steps {
		bundleApp()
		sh 'mvn package'
		copyToHBaseNode()
            }
        }
    }
    post {
        always {
            junit '**/target/*-reports/*.xml'
        }
    }
}

def bundleApp() {
	dir('conf') {
        git(url: "$GITLAB_URL/StatBusReg/sbr-hbase-connector.git", credentialsId: 'sbr-gitlab-id', branch: 'develop')
      }
}

def copyToHBaseNode() {
    echo "Deploying to $ENV"
    sshagent(credentials: ["sbr-$ENV-ci-ssh-key"]) {
        withCredentials([string(credentialsId: "sbr-hbase-node", variable: 'HBASE_NODE')]) {
            sh '''
                    ssh sbr-$ENV-ci@$HBASE_NODE mkdir -p $HBASE_CONNECTOR_DIR/lib
                    scp ${WORKSPACE}/target/*-jar-with-dependencies.jar sbr-$ENV-ci@$HBASE_NODE:$HBASE_CONNECTOR_DIR/lib
					echo "Successfully copied jar file to $HBASE_CONNECTOR_DIR/lib directory on $HBASE_NODE"
					ssh sbr-$ENV-ci@$HBASE_NODE mkdir -p $HBASE_CONNECTOR_DIR/conf
					scp ${WORKSPACE}/conf/$ENV/* sbr-$ENV-ci@$HBASE_NODE:$HBASE_CONNECTOR_DIR/conf
					echo "Successfully copied conf files to $HBASE_CONNECTOR_DIR/conf directory on $HBASE_NODE"
				'''
        }
    }
}
