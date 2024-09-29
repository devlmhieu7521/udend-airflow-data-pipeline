# Data Pipelines with Airflow

Welcome to the Data Pipelines with Airflow project in Udacity! This project will help you to understand about Airflow (DAG, Custom Operator) and ETLs flow.

## Initiating the Airflow Web Server
Ensure [Docker Desktop](https://www.docker.com/products/docker-desktop/) is installed before proceeding.

To bring up the entire app stack up, we use [docker-compose](https://docs.docker.com/engine/reference/commandline/compose_up/) as shown below

```bash
docker-compose up -d
```
Visit http://localhost:8080 once all containers are up and running.

## Create IAM User to have access to AWS S3 and Redshift.
**Step 1:** Go to AWS IAM and click to Users
**Step 2:** Enter the name for IAM Users
**Step 3:** Set permissions:
   - AdministratorAccess
   - AmazonRedshiftFullAccess
   - AmazonS3FullAccess
**Step 4:** Go to the created user and then click to Credential for creating access key.

## Create Redshift Serverless
**Step 1:** Create Redshift Role with full permission to access S3
**Step 2:** Set up Redshift Serverless
**Step 3:** Create Redshift Cluster to get the endpoint

## Configuring Connections in the Airflow Web Server UI

On the Airflow web server UI, use `airflow` for both username and password.
* Post-login, navigate to **Admin > Connections** to add required connections - specifically, `aws_credentials` and `redshift`.
* Don't forget to start your Redshift cluster via the AWS console.
* After completing these steps, run your DAG to ensure all tasks are successfully executed.

## Getting Started with the Project
1. The project template package comprises three key components:
   * The **DAG template** includes imports and task dependencies.
   * The **plugins --> operators** folder with custom operator.
   * A **plugins --> helper class** for SQL transformations.

## DAG Configuration
In the DAG, add `default parameters` based on these guidelines:
* No dependencies on past runs.
* Tasks are retried three times on failure.
* Retries occur every five minutes.
* Catchup is turned off.
* No email on retry.

## Developing Operators
To complete the project, build four operators for staging data, transforming data, and performing data quality checks. While you can reuse code from Project 2, leverage Airflow's built-in functionalities like connections and hooks whenever possible to let Airflow handle the heavy lifting.

### Stage Operator
Load any JSON-formatted files from S3 to Amazon Redshift using the stage operator. The operator should create and run a SQL COPY statement based on provided parameters, distinguishing between JSON files. It should also support loading timestamped files from S3 based on execution time for backfills.

### Fact and Dimension Operators
Utilize the provided SQL helper class for data transformations. These operators take a SQL statement, target database, and optional target table as input. For dimension loads, implement the truncate-insert pattern, allowing for switching between insert modes. Fact tables should support append-only functionality.

### Data Quality Operator
Create the data quality operator to run checks on the data using SQL-based test cases and expected results. The operator should raise an exception and initiate task retry and eventual failure if test results don't match expectations.

After finished, configure task dependencies the flow will represent with the below picture:
![Working DAG with correct task dependencies](assets/final_project_dag_graph.png)