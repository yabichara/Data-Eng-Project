import logging
from pyspark.sql.functions import from_json, col, concat_ws
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType
from pyspark.sql import SparkSession
import psycopg2 

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("spark_streaming.log"),
        logging.StreamHandler()
    ]
)

def create_spark_streaming_session():
    """
    Create a SparkSession configured for Spark Streaming with Kafka.
    """
    try:
        spark_stream_session = SparkSession.builder \
            .appName("LatestMeasurementsStreaming") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4") \
            .config("spark.jars", "/opt/spark/jars/postgresql-42.6.2.jar") \
            .getOrCreate()

        spark_stream_session.sparkContext.setLogLevel("ERROR")
        logging.info("Spark Streaming session created successfully.")
        return spark_stream_session
    except Exception as e:
        logging.error(f"Error creating Spark Streaming session: {e}")
        return None


def process_latest_measurements_stream(spark, kafka_bootstrap_servers, kafka_topic, jdbc_url, username, password):
    """
        Process real-time data from Kafka for the latest measurements and save it to PostgreSQL.

        :param spark: Spark session.
        :param kafka_bootstrap_servers: Kafka bootstrap server address.
        :param kafka_topic: Kafka topic to subscribe to.
        :param jdbc_url: JDBC connection string for PostgreSQL.
        :param username: PostgreSQL username.
        :param password: PostgreSQL password.
    """
    try:
        # Define schema for the streaming data
        schema = StructType([
            StructField("datetime", StructType([
                StructField("utc", StringType(), True),
                StructField("local", StringType(), True)
            ]), True),
            StructField("value", DoubleType(), True),
            StructField("coordinates", StructType([
                StructField("latitude", DoubleType(), True),
                StructField("longitude", DoubleType(), True)
            ]), True),
            StructField("sensorsId", IntegerType(), True),
            StructField("locationsId", IntegerType(), True),
            StructField("end", StringType(), True)  # Field to detect termination signal
        ])

        # Read the Kafka stream
        kafka_stream = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", kafka_topic) \
            .option("startingOffsets", "latest") \
            .load()

        logging.info(f"Is Kafka stream streaming? {kafka_stream.isStreaming}")

        # Parse JSON and process the stream
        parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING) as json") \
            .select(from_json(col("json"), schema).alias("data"))

        # Separate main data stream and termination signal
        data_stream = parsed_stream.filter(col("data.end").isNull())  # Données normales
        end_signal_stream = parsed_stream.filter(col("data.end").isNotNull())  # Signal de fin

        # Transform the data stream
        data_stream = data_stream.select(
            col("data.locationsId").alias("location_id"),
            col("data.sensorsId").alias("sensor_id"),
            col("data.value").alias("value"),
            col("data.datetime.utc").cast("timestamp").alias("timestamp_utc"),
            col("data.datetime.local").cast("timestamp").alias("timestamp_local"),
            col("data.coordinates.latitude").alias("latitude"),
            col("data.coordinates.longitude").alias("longitude")
        ).dropna()

        
        data_stream = data_stream.withColumn(
            "timestamp_utc_str", 
            col("timestamp_utc").cast("string")  # Convert to string format
        )

        # Add unique identifier for upserts
        data_stream = data_stream.withColumn(
            "latest_id", 
            concat_ws("_", col("sensor_id"), col("location_id"), col("timestamp_utc"))
        )

         # Supprimer la colonne temporaire `timestamp_utc_str`   
        data_stream = data_stream.drop("timestamp_utc_str")

        # Reorder columns to place `latest_id` at the beginning
        data_stream = data_stream.select(
            col("latest_id"),
            col("location_id"),
            col("sensor_id"),
            col("value"),
            col("timestamp_utc"),
            col("timestamp_local"),
            col("latitude"),
            col("longitude")
        )

        # Write each batch to PostgreSQL
        def write_to_postgres(batch_df, batch_id):
            """
            Write a batch of data to PostgreSQL with conflict handling and log insert/update operations.
            """
            try:
                if not batch_df.isEmpty():
                    logging.info(f"Processing batch {batch_id} with {batch_df.count()} records.")

                    # Write the batch to a temporary table
                    temp_table_name = "temp_latest_measurements"
                    batch_df.write \
                        .format("jdbc") \
                        .option("url", jdbc_url) \
                        .option("dbtable", temp_table_name) \
                        .option("user", username) \
                        .option("password", password) \
                        .option("driver", "org.postgresql.Driver") \
                        .mode("overwrite") \
                        .save()

                    # Upsert from temporary table to target table
                    upsert_query = f"""
                        WITH upserted AS (
                            INSERT INTO latest_measurements (latest_id, location_id, sensor_id, value, timestamp_utc, timestamp_local, latitude, longitude)
                            SELECT latest_id, location_id, sensor_id, value, timestamp_utc, timestamp_local, latitude, longitude
                            FROM {temp_table_name}
                            ON CONFLICT (latest_id)
                            DO UPDATE SET
                                location_id = EXCLUDED.location_id,
                                sensor_id = EXCLUDED.sensor_id,
                                value = EXCLUDED.value,
                                timestamp_utc = EXCLUDED.timestamp_utc,
                                timestamp_local = EXCLUDED.timestamp_local,
                                latitude = EXCLUDED.latitude,
                                longitude = EXCLUDED.longitude
                            RETURNING xmax = 0 AS is_insert
                        )
                        SELECT
                            COUNT(*) FILTER (WHERE is_insert) AS inserts,
                            COUNT(*) FILTER (WHERE NOT is_insert) AS updates
                        FROM upserted;
                    """

                    with psycopg2.connect(
                        dbname="openaq", user=username, password=password, host="postgres", port=5432
                    ) as conn:
                        with conn.cursor() as cursor:
                            cursor.execute(upsert_query)
                            result = cursor.fetchone()
                            conn.commit()

                    # Log the number of inserts and updates
                    inserts, updates = result
                    logging.info(f"Batch {batch_id}: {inserts} rows inserted, {updates} rows updated.")
                else:
                    logging.info(f"No data to process for batch {batch_id}. Skipping.")
                    return
                
            except Exception as e:
                logging.error(f"Error writing batch {batch_id} to PostgreSQL: {e}")


        # Write the streaming data
        data_query = data_stream.writeStream \
            .foreachBatch(write_to_postgres) \
            .outputMode("append") \
            .option("checkpointLocation", "/tmp/spark-checkpoints-data") \
            .start()
        return data_query
    except Exception as e:
        logging.error(f"Error in Spark Streaming processing: {e}")
        raise


def main_spark_stream():
    """
        Entry point for executing the Spark Streaming pipeline.
        
        This function creates a Spark Streaming session, processes the latest 
        measurements data from Kafka, and writes it to a PostgreSQL database. 
        The streaming query listens indefinitely until an explicit stop or an error occurs.
    """
    # Parameters
    KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
    KAFKA_TOPIC = "openaq-latest-measurements"
    JDBC_URL = "jdbc:postgresql://postgres:5432/openaq"
    USERNAME = "your_user"
    PASSWORD = "your_password"

    logging.info("Initializing Spark Streaming session...")

    # Create a Spark Streaming session
    spark_session = create_spark_streaming_session()

    if spark_session:
        logging.info("Spark Streaming session created successfully.")
        
        query = None
        try:
            # Process the Kafka stream and start the query
            logging.info("Starting Spark Streaming query...")
            query = process_latest_measurements_stream(
                spark_session,
                KAFKA_BOOTSTRAP_SERVERS,
                KAFKA_TOPIC,
                JDBC_URL,
                USERNAME,
                PASSWORD
            )
            query.awaitTermination()  # Wait for streaming events or an explicit stop
        except Exception as e:
            logging.error(f"Error during Spark Streaming query: {e}")
        finally:
            if query:
                logging.info("Stopping Spark Streaming query...")
                query.stop()  # Stop the streaming explicitly
            if spark_session:
                logging.info("Stopping Spark session...")
                spark_session.stop()  # Ensure the Spark session is stopped
            logging.info("Spark Streaming pipeline terminated.")
    else:
        logging.error("Failed to create Spark Streaming session. Exiting.")

if __name__ == "__main__":
    main_spark_stream()