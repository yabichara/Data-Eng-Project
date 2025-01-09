import json
import logging
import asyncio
from confluent_kafka import Producer, Consumer, TopicPartition

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'kafka:9092',
    'client.id': 'openaq-producer',
    'group.id': 'openaq-group',
    'auto.offset.reset': 'earliest'
}

TOPICS = {
    "country_data": "openaq-country-data",
    "locations_data": "openaq-locations-data",
    "latest_measurements": "openaq-latest-measurements",
    "parameter_data": "openaq-parameter-data"
}

# Initialize the Kafka producer
def create_producer():
    try:
        return Producer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap.servers'],
            'message.max.bytes': 10 * 1024 * 1024  # 10 MB
        })
    except Exception as e:
        logging.error(f"Error creating Kafka producer: {e}")
        return None

# Initialize the Kafka consumer
def create_consumer():
    try:
        return Consumer({
            'bootstrap.servers': KAFKA_CONFIG['bootstrap.servers'],
            'group.id': 'deduplication-checker',
            'auto.offset.reset': 'earliest',
            'fetch.message.max.bytes': 10 * 1024 * 1024
        })
    except Exception as e:
        logging.error(f"Error creating Kafka consumer: {e}")
        return None

def create_kafka_client(client_type="producer", client_config=None):
    """
    Factory function to create Kafka Producer or Consumer.

    :param client_type: 'producer' or 'consumer'.
    :param client_config: Configuration dictionary for Kafka client.
    :return: Kafka Producer or Consumer instance.
    """
    client_config = client_config or KAFKA_CONFIG
    try:
        if client_type == "producer":
            return Producer(
                {
                    "bootstrap.servers": client_config["bootstrap.servers"],
                    "message.max.bytes": 10 * 1024 * 1024,
                }
            )
        elif client_type == "consumer":
            return Consumer(
                {
                    "bootstrap.servers": client_config["bootstrap.servers"],
                    "group.id": "deduplication-checker",
                    "auto.offset.reset": "earliest",
                    "fetch.message.max.bytes": 10 * 1024 * 1024,
                }
            )
        else:
            raise ValueError(f"Unknown client type: {client_type}")
    except Exception as e:
        logging.error(f"Error creating Kafka {client_type}: {e}")
        return None
    
def delivery_report(err, msg):
    """
    Callback function for Kafka message delivery.

    :param err: Error details if the delivery failed.
    :param msg: The Kafka message being sent.
    """
    if err:
        logging.error(f"Message delivery failed: {err}")
    else:
        logging.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def is_topic_empty(consumer, topic):
    """
        Check if a Kafka topic is empty.

        :param consumer: Kafka consumer instance.
        :param topic: Kafka topic name.
        :return: True if topic is empty, False otherwise.
    """
    try:
        metadata = consumer.list_topics(topic, timeout=5)
        partitions = metadata.topics[topic].partitions

        for partition_id in partitions.keys():
            tp = TopicPartition(topic=topic, partition=partition_id)
            low, high = consumer.get_watermark_offsets(tp, timeout=5)
            if high > low:
                logging.info(
                    f"Messages found in topic {topic}, partition {partition_id}. Offsets: low={low}, high={high}"
                )
                return False

        logging.info(f"Topic {topic} is empty.")
        return True
    except Exception as e:
        logging.error(f"Error checking topic {topic}: {e}")
        return False

# Publish data to Kafka only if the topic is empty
def publish_to_kafka_if_empty(producer, topic, data):
    """
        Publishes data to a Kafka topic if it is empty.

        :param producer: Kafka producer instance.
        :param topic: Kafka topic name.
        :param data: Data to be published.
    """
    # consumer = create_consumer()
    consumer = create_kafka_client("consumer")
    if not consumer:
        logging.error("Consumer could not be created. Skipping publishing.")
        return

    try:
        if is_topic_empty(consumer, topic):
            producer.produce(
                topic,
                value=json.dumps(data),
                callback=delivery_report
            )
            producer.flush()
            logging.info(f"Data published to topic {topic}.")
        else:
            logging.info(f"Topic {topic} already contains data. Skipping publishing.")
    except Exception as e:
        logging.error(f"Error publishing to topic {topic}: {e}")
    finally:
        consumer.close()

async def fetch_and_publish_country_locations_data(collector, country_id, producer):
    """
        Fetches and publishes country, parameter, and location data to Kafka.

        :param collector: OpenAQDataCollector instance for fetching data.
        :param country_id: ID of the country for fetching data.
        :param producer: Kafka producer instance for publishing data.
    """
    try:
        logging.info("Fetching country data...")
        country_data = await collector.get_country_data(country_id)
        if country_data:
            # Fetch associated parameter data
            parameters_data = await collector.get_parameter_data([param["id"] for param in country_data["parameters"]])

            # Publish country and parameter data to Kafka
            publish_to_kafka_if_empty(producer, TOPICS["country_data"], country_data)
            publish_to_kafka_if_empty(producer, TOPICS["parameter_data"], parameters_data)
            logging.info("Country and parameter data published.")

        logging.info("Fetching location data...")
        locations_data = await collector.get_locations_data(country_id)

        if locations_data:
            # Publish location data to Kafka
            publish_to_kafka_if_empty(producer, TOPICS["locations_data"], locations_data)
            logging.info("Location data published.")
    except Exception as e:
        logging.error(f"Error fetching and publishing data: {e}")


async def fetch_and_publish_data(collector, producer):
    """
        Main asynchronous function to fetch and publish country, parameter, and location data.

        :param collector: OpenAQDataCollector instance.
        :param producer: Kafka producer instance.
    """
    try:
        if producer:
            logging.info("Kafka producer created successfully.")
            await fetch_and_publish_country_locations_data(collector, country_id=22, producer=producer)
        else:
            logging.error("Failed to create Kafka producer.")

        return collector.locations
    except Exception as e:
        logging.error(f"Error in fetch_and_publish_data: {e}")

def fetch_and_publish_dimension_data(collector, producer):
    """
        Wrapper function to execute the fetch and publish logic synchronously.

        :param collector: OpenAQDataCollector instance.
        :param producer: Kafka producer instance.
    """
    try:
        logging.info("Starting Kafka producer...")
        return asyncio.run(fetch_and_publish_data(collector, producer))
    except Exception as e:
        logging.error(f"Error running mainKafkaDimensionData: {e}")


def publish_measurements_to_kafka(producer, topic, measurements):
    """
    Publishes measurements data to Kafka synchronously using confluent_kafka.

    :param producer: Confluent Kafka producer instance.
    :param topic: Kafka topic name.
    :param measurements: List of measurement data to publish.
    """
    try:
        for measurement in measurements:
            message = json.dumps(measurement).encode("utf-8")
            producer.produce(topic, message, callback=delivery_report)

        # Ensure all messages are sent
        producer.flush()
        logging.info(f"Published {len(measurements)} messages to Kafka topic '{topic}'.")
    except Exception as e:
        logging.error(f"Error publishing to Kafka: {e}")

async def fetch_and_publish_latest_measurements(collector, producer, kafka_topic):
    """
    Fetches and publishes latest measurements incrementally to Kafka.

    :param collector: OpenAQDataCollector instance.
    :param producer: Kafka producer instance.
    :param kafka_topic: Kafka topic name.
    """
    try:
        async for batch in collector.get_latest_measurements(collector.locations):
            if batch:
                logging.info(f"Fetched batch of {len(batch)} measurements.")

                # Publish each batch to Kafka as it's fetched
                publish_measurements_to_kafka(producer, kafka_topic, batch)
                logging.info(f"Published batch of {len(batch)} measurements to Kafka.")
            
        # Publish a signal to indicate the end of data publishing
        end_signal = {"end": True}
        publish_measurements_to_kafka(producer, kafka_topic, [end_signal])
        logging.info("End signal published to Kafka.")    
    except Exception as e:
        logging.error(f"Error in fetch-and-publish loop: {e}")