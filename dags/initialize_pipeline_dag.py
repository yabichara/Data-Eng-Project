from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta
import sys
import os
import asyncio



current_dir = os.path.dirname(os.path.abspath(__file__))
spark_path = os.path.join(current_dir, "../spark")
sys.path.append(spark_path)

from spark_stream import logging
current_dir = os.path.dirname(os.path.abspath(__file__))
kafka_scripts_path = os.path.join(current_dir, "../kafkaScripts")
sys.path.append(kafka_scripts_path)
from OpenAQCollector import OpenAQDataCollector
from KafkaProducer import (
    fetch_and_publish_latest_measurements,
    json,
    create_kafka_client,
    fetch_and_publish_dimension_data)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'fetch_and_publish_to_kafka',
    default_args=default_args,
    description='Initialize the pipeline, fetch OpenAQ data, and publish to Kafka incrementally.',
    schedule_interval="*/20 * * * *", # Schedule every 20 minutes
    start_date=datetime(2025, 1, 8),
    catchup=False,
) as dag:
    def initialize_the_pipeline():
        """
        Initialize the pipeline by creating a Kafka producer and fetching initial dimension data.
        """
        try:
            with open("/opt/airflow/api_key.json", "r") as api_file:
                api_key = json.load(api_file)["New_API_KEY"]
            
            # Initialize API collector and Kafka producer
            collector = OpenAQDataCollector(api_key)
            producer = create_kafka_client()
            logging.info("Starting Kafka producer...")
            # Fetch and publish dimension data
            locations = fetch_and_publish_dimension_data(collector, producer)
            return locations
        
        except Exception as e:
            logging.error(f"Error during pipeline initialization: {e}")
            return None, None
        
    def execute_fetch_and_publish(**kwargs):
        """
            Fetch the latest measurements from OpenAQ and publish them to Kafka in real-time.
        """
        try:
            # Récupérer les données depuis XCom
            ti = kwargs['ti']
            locations = ti.xcom_pull(task_ids='producer_dimension_data')

            with open("/opt/airflow/api_key.json", "r") as api_file:
                api_key = json.load(api_file)["New_API_KEY"]
            
            # Initialize API collector and Kafka producer
            collector = OpenAQDataCollector(api_key)
            producer = create_kafka_client()
            collector.locations = locations

            # Publish latest measurements incrementally to Kafka
            kafka_topic = "openaq-latest-measurements"
            asyncio.run(fetch_and_publish_latest_measurements(collector, producer, kafka_topic))

        except Exception as e:
            logging.error(f"Error processing data: {e}")
            raise e

    # Task 1: Fetch dimension data and initialize the pipeline
    python_task = PythonOperator(
        task_id="producer_dimension_data",
        python_callable=initialize_the_pipeline,
        do_xcom_push=True  # Enable XCom for passing data between tasks
    )


    # Task 2: Process dimension data using Spark
    spark_task = SparkSubmitOperator(
    task_id="initialize_pipeline_task",
    application="/opt/airflow/spark/spark_process_dim_data.py",  # Assurez-vous que ce chemin est correct
    conn_id="spark_default",
    jars="/opt/airflow/jars/postgresql-42.6.2.jar",
    packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4",
    executor_cores=2,
    executor_memory="2g",
    driver_memory="1g",
    name="initialize_pipeline_task",
    verbose=True,
    conf={"spark.master": "local[*]"},
    dag=dag,
    )

    # Task 3: Fetch and publish latest measurement data to Kafka
    fetch_and_publish_task = PythonOperator(
        task_id='fetch_and_publish_task',
        python_callable=execute_fetch_and_publish
    )

    # Define task execution order
    python_task >> spark_task >> fetch_and_publish_task
