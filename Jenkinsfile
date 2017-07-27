pipeline {
    agent any
    stages {
        stage('Build') {
            steps {
                sh 'mvn clean compile test'
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
    }
    post {
        always {
            junit '**/target/*-reports/*.xml'
            deleteDir()
        }
    }
}
