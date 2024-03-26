from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import json
import os
import time
import uuid
import boto3

# Define job configuration from the provided event
job_config = {
    "jar": {{jar}},
    "spark_submit_parameters": {{spark_submit_parameters}},
    "arguments": {{arguments}},
    "job": {{ job }},
}

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 3, 20),
}


def check_job_status(client, run_id, applicationId):
    response = client.get_job_run(applicationId=applicationId, jobRunId=run_id)
    return response['jobRun']['state']


def lambda_handler(event, context):
    # Create EMR serverless client object
    client = boto3.client("emr-serverless",
                          aws_access_key_id=os.getenv("DEV_ACCESS_KEY"),
                          aws_secret_access_key=os.getenv("DEV_SECRET_KEY"),
                          region_name=os.getenv("DEV_REGION"))

    # Extracting parameters from the event
    jar = event.get("jar", [])
    # Add --conf spark.jars with comma-separated values from the jar object
    spark_submit_parameters = ' '.join(event.get("spark_submit_parameters", []))  # Convert list to string
    spark_submit_parameters = f'--conf spark.jars={",".join(jar)} {spark_submit_parameters}'  # Join with existing parameters

    arguments = event.get("arguments", {})
    job = event.get("job", {})

    # Extracting job details
    JobName = job.get("job_name")
    ApplicationId = job.get("ApplicationId")
    ExecutionTime = job.get("ExecutionTime")
    ExecutionArn = job.get("ExecutionArn")

    # Processing arguments
    entryPointArguments = []
    for key, value in arguments.items():
        if key == "hoodie-conf":
            # Extract hoodie-conf key-value pairs and add to entryPointArguments
            for hoodie_key, hoodie_value in value.items():
                entryPointArguments.extend(["--hoodie-conf", f"{hoodie_key}={hoodie_value}"])
        elif isinstance(value, bool):
            # Add boolean parameters without values if True
            if value:
                entryPointArguments.append(f"--{key}")
        else:
            entryPointArguments.extend([f"--{key}", f"{value}"])

    # Starting the EMR job run
    response = client.start_job_run(
        applicationId=ApplicationId,
        clientToken=str(uuid.uuid4()),
        executionRoleArn=ExecutionArn,
        jobDriver={
            'sparkSubmit': {
                'entryPoint': "command-runner.jar",
                'entryPointArguments': entryPointArguments,
                'sparkSubmitParameters': spark_submit_parameters
            },
        },
        executionTimeoutMinutes=ExecutionTime,
        name=JobName
    )

    if job.get("JobStatusPolling").__str__().lower() == "true":
        # Polling for job status
        run_id = response['jobRunId']
        print("Job run ID:", run_id)

        polling_interval = 2
        while True:
            status = check_job_status(client=client, run_id=run_id, applicationId=ApplicationId)
            print("Job status:", status)
            if status in ["FAILED"]:
                raise Exception("DeltaStreamer Job Failed")

            if status in ["CANCELLED", "SUCCESS"]:
                break
            time.sleep(polling_interval)  # Poll every 3 seconds

    return {
        "statusCode": 200,
        "body": json.dumps(response)
    }


# Function to execute Lambda handler
def execute_lambda_handler(event, context):
    return lambda_handler(event, context)


# Define Airflow DAG
def create_dag(job_config):
    dag_name = f"dag_{job_config['job']['job_name']}"
    schedule = job_config['job']['schedule']

    dag = DAG(
        dag_id=dag_name,
        default_args=default_args,
        description=job_config['job']['JobDescription'],
        schedule_interval=schedule,
    )

    # Define task to execute Lambda handler
    execute_lambda_task = PythonOperator(
        task_id='execute_lambda_task',
        python_callable=execute_lambda_handler,
        op_kwargs={'event': job_config, 'context': None},
        dag=dag,
    )

    return dag


# Create the DAG
dag = create_dag(job_config)
