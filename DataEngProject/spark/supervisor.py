import requests
import logging
import subprocess
from time import sleep


# Airflow configurations
AIRFLOW_BASE_URL = "http://airflow-webserver:8086/api/v1"
DAG_ID = "fetch_and_publish_to_kafka"
TASK_ID = "fetch_and_publish_task"
USERNAME = "admin"
PASSWORD = "admin"

# PySpark script to execute
SPARK_SCRIPT_PATH = "/opt/spark-apps/spark/spark_stream.py"

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("supervisor.log")
    ]
)

def get_latest_dag_run():
    """
    Fetch the most recent `dag_run_id` for the specified DAG.

    :return: The ID of the latest DAG run or None if not found.
    """

    try:
        url = f"{AIRFLOW_BASE_URL}/dags/{DAG_ID}/dagRuns"
        params = {"order_by": "-execution_date", "limit": 1}
        response = requests.get(url, params=params, auth=(USERNAME, PASSWORD))
        response.raise_for_status()
        dag_runs = response.json()
        if dag_runs["dag_runs"]:
            return dag_runs["dag_runs"][0]["dag_run_id"]
        return None
    except requests.RequestException as e:
        logging.error(f"Error retrieving the latest DAG run: {e}")
        return None

def get_task_instance_status(dag_run_id):
    """
        Retrieve the status of a task for a specific DAG run.

        :param dag_run_id: The ID of the DAG run.
        :return: The task state as a string or None if an error occurs.
    """
    try:
        url = f"{AIRFLOW_BASE_URL}/dags/{DAG_ID}/dagRuns/{dag_run_id}/taskInstances/{TASK_ID}"
        response = requests.get(url, auth=(USERNAME, PASSWORD))
        response.raise_for_status()
        task_instance = response.json()
        return task_instance.get("state")
    except requests.RequestException as e:
        logging.error(f"Error retrieving task status: {e}")
        return None

def launch_spark_streaming():
    """
        Launch the PySpark script using `spark-submit`.
    """
    jars_path = "/opt/spark/jars/postgresql-42.6.2.jar"
    packages = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4"
    try:
        logging.info("Launching the PySpark script...")
        subprocess.run(
            [
                "spark-submit",
                "--master", "local[*]",
                "--jars", jars_path,
                "--packages", packages,
                SPARK_SCRIPT_PATH
            ],
            check=True
        )
        logging.info("PySpark script executed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error during PySpark script execution: {e}")
    except Exception as e:
        logging.error(f"Unexpected error during Spark execution: {e}")


def main():
    """
    Main function to monitor the Airflow task and trigger Spark Streaming accordingly.
    """
    logging.info("Monitoring the Airflow task...")
    while True:
        dag_run_id = get_latest_dag_run()
        if dag_run_id:
            logging.info(f"Latest DAG run ID: {dag_run_id}")
            state = get_task_instance_status(dag_run_id)
            if state:
                logging.info(f"Task '{TASK_ID}' state: {state}")
                if state == "running":
                    logging.info("Triggering Spark Streaming...")
                    launch_spark_streaming()
                elif state == "success":
                    logging.info("Task completed successfully. Waiting for next dag run.")
                    # Add code here to stop Spark Streaming if necessary.
                else:
                    logging.warning(f"Task in an unexpected state: {state}")
            else:
                logging.warning("Failed to retrieve task state.")
        else:
            logging.warning("No DAG runs found.")

        sleep(10)  # Add a delay between checks to avoid overloading the server

if __name__ == "__main__":
    main()