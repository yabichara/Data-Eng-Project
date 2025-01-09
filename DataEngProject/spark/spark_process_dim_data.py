from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, explode, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, ArrayType, MapType
import psycopg2 
import sys
import os
from typing import Optional, Tuple
# Ajouter le chemin absolu de 'kafkaScripts' au PYTHONPATH
current_dir = os.path.dirname(os.path.abspath(__file__))
kafka_scripts_path = os.path.join(current_dir, "../kafkaScripts")
sys.path.append(kafka_scripts_path)

from OpenAQCollector import logging

def create_spark_connection() -> Optional[SparkSession]:
    """
    Establishes a SparkSession for distributed data processing with configurations
    for Kafka and PostgreSQL connectors.

    :return: SparkSession object if successful, None otherwise.
    """
    try:
        spark_session  = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config("spark.jars", "/opt/airflow/jars/postgresql-42.6.2.jar")
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4")
            .getOrCreate()
        )
        spark_session .sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
        return spark_session 
    except Exception as e:
        logging.error(f"Failed to create Spark session: {e}")
        return None

def connect_to_kafka_static(spark_session: SparkSession, topic: str):
    """
    Connects Spark to a Kafka topic and retrieves messages into a DataFrame.

    :param spark_conn: Active SparkSession.
    :param topic: Kafka topic to subscribe to.
    :return: DataFrame containing `value` and `timestamp` columns, or None on failure.
    """
    try:
        kafka_df = (
            spark_session.read.format("kafka")
            .option("kafka.bootstrap.servers", "kafka:9092")
            .option("subscribe", topic)
            .option("startingOffsets", "earliest")
            .load()
        )
        logging.info("Kafka DataFrame created successfully")
        return kafka_df.select(
            col("value").cast("string").alias("value"),
            col("timestamp")
        )
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        return None

def process_and_enrich_country_data(country_df, parameters_df):
    """
        Processes and enriches raw country data by integrating parameter details.

        Steps:
        1. Parses the country JSON data into a structured DataFrame.
        2. Explodes the `parameters` array to normalize the data.
        3. Selects and renames key columns for clarity.
        4. Parses parameter data, extracting and formatting relevant fields.
        5. Performs a left join to enrich the country data with parameter details.

        :param country_df: Spark DataFrame containing raw country data as JSON strings.
        :param parameters_df: Spark DataFrame containing raw parameter data as JSON strings.
        :return: A Spark DataFrame containing processed and enriched country data.
    """
    # Define schema for country data
    country_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("name", StringType(), True),
        StructField("datetimeFirst", TimestampType(), True),
        StructField("datetimeLast", TimestampType(), True),
        StructField("parameters", ArrayType(StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("units", StringType(), True),
            StructField("displayName", StringType(), True),
            StructField("description", StringType(), True)
        ])))
    ])

    # Define schema for parameters data
    parameters_schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("units", StringType(), True),
        StructField("displayName", StringType(), True),
        StructField("description", StringType(), True),
    ])

    # Pre-process country data
    processed_country = country_df.withColumn(
        "parsed_value", from_json(col("value").cast("string"), country_schema)
    ).select(
        col("parsed_value.id").alias("id"),
        col("parsed_value.code").alias("code"),
        col("parsed_value.name").alias("name"),
        col("parsed_value.datetimeFirst").alias("datetimeFirst"),
        col("parsed_value.datetimeLast").alias("datetimeLast"),
        explode(col("parsed_value.parameters")).alias("parameter")
    ).select(
        col("id"),
        col("code"),
        col("name"),
        col("datetimeFirst"),
        col("datetimeLast"),
        col("parameter.id").alias("parameter_id"),
        col("parameter.name").alias("parameter_name"),
        col("parameter.units").alias("parameter_units"),
        col("parameter.displayName").alias("parameter_display_name"),
        col("parameter.description").alias("parameter_description")
    )

    # Process parameter data
    processed_parameters = parameters_df.select(
        explode(from_json(col("value").cast("string"), ArrayType(parameters_schema))).alias("parsed_value")
    ).select(
        col("parsed_value.id").alias("id"),
        col("parsed_value.displayName").alias("displayName"),
        col("parsed_value.description").alias("description")
    )

    # Enrich country data with parameter details
    enriched_country = processed_country.join(
        processed_parameters,
        processed_country["parameter_id"] == processed_parameters["id"],
        "left_outer"
    ).select(
        processed_country["id"],
        processed_country["code"],
        processed_country["name"],
        processed_country["datetimeFirst"],
        processed_country["datetimeLast"],
        processed_country["parameter_id"],
        processed_country["parameter_name"],
        processed_country["parameter_units"],
        col("displayName").alias("parameter_display_name"),
        col("description").alias("parameter_description")
    )

    return enriched_country

def write_to_temp_table(df, jdbc_url, temp_table_name, username, password, schema_specific_casts=None):
    """
        Writes a Spark DataFrame to a PostgreSQL temporary table.

        Steps:
        1. Optionally casts specific columns to the desired schema.
        2. Writes the DataFrame into a temporary table using the JDBC connector.
        3. Overwrites the table if it already exists.

        :param df: Spark DataFrame to write to the database.
        :param jdbc_url: PostgreSQL JDBC connection URL.
        :param temp_table_name: Name of the temporary table to create in PostgreSQL.
        :param username: Database username.
        :param password: Database password.
        :param schema_specific_casts: Optional dictionary specifying columns to cast and their target data types.
    """
    try:
        if schema_specific_casts:
            for col_name, cast_type in schema_specific_casts.items():
                if col_name in df.columns:
                    df = df.withColumn(col_name, col(col_name).cast(cast_type))
        df.write.format("jdbc").options(
            url=jdbc_url,
            dbtable=temp_table_name,
            user=username,
            password=password,
            driver="org.postgresql.Driver"
        ).mode("overwrite").save()
        logging.info(f"Data successfully written to temporary table {temp_table_name}.")
    except Exception as e:
        logging.error(f"Error writing to temporary table {temp_table_name}: {e}")

def upsert_from_temp_to_target(jdbc_url, temp_table_name, target_table_name, username, password, columns, conflict_column="id"):
    """
    Performs an upsert operation from a temporary table to the target table.

    :param jdbc_url: PostgreSQL JDBC URL.
    :param temp_table_name: Temporary table name.
    :param target_table_name: Target table name.
    :param username: Database username.
    :param password: Database password.
    :param columns: Columns involved in the upsert.
    :param conflict_column: Column to handle conflicts (default: "id").
    """
    try:
        host = jdbc_url.split("//")[1].split(":")[0]
        port = jdbc_url.split(":")[-1].split("/")[0]
        database = jdbc_url.split("/")[-1]

        conn = psycopg2.connect(dbname=database, user=username, password=password, host=host, port=port)
        cursor = conn.cursor()

        column_names = ", ".join([f'"{col}"' for col in columns])
        excluded_columns = ", ".join([f'"{col}" = EXCLUDED."{col}"' for col in columns if col != conflict_column])

        upsert_query = f"""
        INSERT INTO "{target_table_name}" ({column_names})
        SELECT {column_names} FROM "{temp_table_name}"
        ON CONFLICT ("{conflict_column}") DO UPDATE
        SET {excluded_columns};
        """
        cursor.execute(upsert_query)
        conn.commit()

        cursor.execute(f'DROP TABLE IF EXISTS "{temp_table_name}";')
        conn.commit()

        logging.info(f"Upsert completed from {temp_table_name} to {target_table_name}.")
    except Exception as e:
        logging.error(f"Error during upsert from {temp_table_name} to {target_table_name}: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def write_country_and_parameter_data(processed_country, jdbc_url, username, password):
    """
    Processes and upserts country and parameter data.

    :param processed_country: Spark DataFrame with processed country data.
    :param jdbc_url: PostgreSQL JDBC URL.
    :param username: Database username.
    :param password: Database password.
    """
    country_columns = ["id", "code", "name", "datetimeFirst", "datetimeLast"]
    parameter_columns = ["parameter_id", "name", "unit", "display_name", "description"]

    country_df = processed_country.select("id", "code", "name", "datetimeFirst", "datetimeLast").dropDuplicates(["id"])
    parameter_df = processed_country.select(
        col("parameter_id"),
        col("parameter_name").alias("name"),
        col("parameter_units").alias("unit"),
        col("parameter_display_name").alias("display_name"),
        col("parameter_description").alias("description")
    ).dropDuplicates(["parameter_id"])

    write_to_temp_table(country_df, jdbc_url, "temp_processed_country_data", username, password)
    write_to_temp_table(parameter_df, jdbc_url, "temp_processed_parameter_data", username, password)

    upsert_from_temp_to_target(jdbc_url, "temp_processed_country_data", "country", username, password, country_columns)
    upsert_from_temp_to_target(jdbc_url, "temp_processed_parameter_data", "parameter", username, password, parameter_columns, conflict_column="parameter_id")

def pre_process_location_data(location_df):
    """
    Processes and normalizes location data from a Spark DataFrame containing an array of dictionaries.

    :param location_df: Spark DataFrame containing a "value" column with an array of dictionaries (JSON format).
    :return: Spark DataFrame with normalized sensor data, or None if an error occurs.
    """
    try:
        # Define schema for parsing location data
        location_schema = StructType([
            StructField("id", IntegerType(), True),
            StructField("name", StringType(), True),
            StructField("locality", StringType(), True),
            StructField("timezone", StringType(), True),
            StructField("country", StructType([
                StructField("id", IntegerType(), True),
                StructField("code", StringType(), True)
            ]), True),
            StructField("coordinates", MapType(StringType(), StringType()), True),
            StructField("datetimeFirst", StructType([
                StructField("utc", StringType(), True),
                StructField("local", StringType(), True)
            ]), True),
            StructField("datetimeLast", StructType([
                StructField("utc", StringType(), True),
                StructField("local", StringType(), True)
            ]), True),
            StructField("sensors", ArrayType(StructType([
                StructField("id", IntegerType(), True),
                StructField("name", StringType(), True),
                StructField("parameter", StructType([
                    StructField("id", IntegerType(), True)
                ]), True)
            ])), True)
        ])

        # Parse the "value" column and explode the parsed array
        parsed_df = location_df.withColumn(
            "parsed_value", from_json(col("value").cast("string"), ArrayType(location_schema))
        )
        exploded_df = parsed_df.withColumn("location", explode(col("parsed_value")))

        # Extract fields from JSON and normalize datetimeFirst and datetimeLast
        location_data = exploded_df.select(
            col("location.id").alias("Location ID"),
            col("location.name").alias("Name"),
            col("location.locality").alias("Locality"),
            col("location.country.id").alias("Country ID"),
            col("location.timezone").alias("Timezone"),
            col("location.coordinates.latitude").cast("double").alias("Latitude"),
            col("location.coordinates.longitude").cast("double").alias("Longitude"),
            col("location.datetimeFirst.utc").cast("timestamp").alias("datetime_first_utc"),
            col("location.datetimeFirst.local").cast("timestamp").alias("datetime_first_local"),
            col("location.datetimeLast.utc").cast("timestamp").alias("datetime_last_utc"),
            col("location.datetimeLast.local").cast("timestamp").alias("datetime_last_local"),
            col("location.sensors").alias("Sensors")
        )

        # Explode the sensors array and extract sensor details
        sensors_data = location_data.withColumn("Sensor", explode(col("Sensors"))).select(
            col("Location ID"),
            col("Name"),
            col("Locality"),
            col("Country ID"),
            col("Timezone"),
            col("Latitude"),
            col("Longitude"),
            col("datetime_first_utc"),
            col("datetime_first_local"),
            col("datetime_last_utc"),
            col("datetime_last_local"),
            col("Sensor.id").alias("Sensor ID"),
            col("Sensor.name").alias("Sensor Name"),
            col("Sensor.parameter.id").alias("Parameter ID")
        )

        return sensors_data

    except Exception as e:
        logging.error(f"Error processing location data: {e}")
        return None


def process_locations_df(df):
    """
    Splits location data into two DataFrames: one for location details and one for sensor details.

    :param df: Pre-processed Spark DataFrame containing location data and sensors.
    :return: Tuple of two DataFrames:
             - location_df: Contains normalized location data.
             - sensors_df: Contains normalized sensor data.
    """
    location_df = df.select(
        col("Location ID").alias("location_id"),
        col("Name").alias("name"),
        col("Locality").alias("locality"),
        col("Country ID").alias("country_id"),
        col("Latitude").alias("latitude"),
        col("Longitude").alias("longitude"),
        col("Timezone").alias("timezone"),
        col("datetime_first_utc"),
        col("datetime_first_local"),
        col("datetime_last_utc"),
        col("datetime_last_local")
    )

    sensors_df = df.select(
        col("Sensor ID").alias("sensor_id"),
        col("Sensor Name").alias("name"),
        col("Parameter ID").alias("parameter_id"),
        col("Location ID").alias("location_id")
    )

    return location_df, sensors_df


def write_location_and_sensor_data(processed_location, processed_sensor, jdbc_url, username, password):
    """
    Writes processed location and sensor data into PostgreSQL tables via temporary tables and upsert logic.

    :param processed_location: Spark DataFrame containing processed location data.
    :param processed_sensor: Spark DataFrame containing processed sensor data.
    :param jdbc_url: PostgreSQL JDBC URL.
    :param username: Database username.
    :param password: Database password.
    """
    # Define table-specific columns
    location_columns = [
        "location_id", "name", "locality", "country_id", "latitude", "longitude",
        "timezone", "datetime_first_utc", "datetime_first_local",
        "datetime_last_utc", "datetime_last_local"
    ]
    sensor_columns = ["sensor_id", "name", "parameter_id", "location_id"]

    # Write data into temporary tables
    write_to_temp_table(processed_location, jdbc_url, "temp_processed_location_data", username, password)
    write_to_temp_table(processed_sensor, jdbc_url, "temp_processed_sensor_data", username, password)

    # Upsert data into target tables
    upsert_from_temp_to_target(
        jdbc_url,
        "temp_processed_location_data",
        "location_table",
        username,
        password,
        location_columns,
        conflict_column="location_id"
    )
    upsert_from_temp_to_target(
        jdbc_url,
        "temp_processed_sensor_data",
        "sensor",
        username,
        password,
        sensor_columns,
        conflict_column="sensor_id"
    )

def sparkDimDagTask():
    """
        Main task for processing and storing dimension data using Spark and Kafka.
        - Reads data from Kafka topics.
        - Processes and enriches country, parameter, location, and sensor data.
        - Writes the processed data into PostgreSQL tables.

        :return: None
    """
    logging.info("Creating Spark connection...")
    spark_conn = create_spark_connection()

    if spark_conn is None:
        logging.error("Failed to create Spark connection. Exiting...")
        return
    
    # Database credentials and Kafka topics
    jdbc_url = "jdbc:postgresql://postgres:5432/openaq"
    username = "your_user"
    password = "your_password"
    country_topic = "openaq-country-data"
    parameter_topic = "openaq-parameter-data"
    location_topic = "openaq-locations-data"

    try:
        # Step 1: Read data from Kafka
        logging.info("Connecting to Kafka topics...")
        kafka_country_df = connect_to_kafka_static(spark_conn, country_topic)
        kafka_parameter_df = connect_to_kafka_static(spark_conn, parameter_topic)
        kafka_locations_df = connect_to_kafka_static(spark_conn, location_topic)

        # Step 2: Process and write country and parameter data
        if kafka_country_df and kafka_parameter_df:
            logging.info("Processing country and parameter data...")
            processed_country = process_and_enrich_country_data(kafka_country_df, kafka_parameter_df)

            if processed_country:
                logging.info("Writing country and parameter data to PostgreSQL...")
                write_country_and_parameter_data(processed_country, jdbc_url, username, password)
            else:
                logging.warning("Processed country data is empty. Skipping write operation.")

        # Step 3: Process and write location and sensor data
        if kafka_locations_df:
            logging.info("Processing location data...")
            processed_locations_df  = pre_process_location_data(kafka_locations_df)

            if processed_locations_df:
                # Split into locations and sensors DataFrames
                locations_df, sensors_df  = process_locations_df(processed_locations_df)

                # Remove duplicates for a clean dataset
                locations_df = locations_df.dropDuplicates(["location_id"])
                sensors_df = sensors_df.dropDuplicates(["sensor_id"])

                # Write data to PostgreSQL
                if not locations_df.isEmpty() and not sensors_df.isEmpty():
                    logging.info("Writing location and sensor data to PostgreSQL...")
                    write_location_and_sensor_data(locations_df, sensors_df, jdbc_url, username, password)
                else:
                    logging.warning("Location or sensor data is empty. Skipping write operation.")
        else:
            logging.warning("Processed locations data is empty. Skipping further processing.")
    except Exception as e:
        logging.error(f"An error occurred during Spark dimension data processing: {e}")

if __name__ == "__main__":
    sparkDimDagTask()