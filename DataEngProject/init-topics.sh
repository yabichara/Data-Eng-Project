#!/bin/bash

# Attendre que Kafka soit prêt
echo "Waiting for Kafka to be ready..."
while ! nc -z kafka 9092; do
  sleep 1
done
# Créer les topics nécessaires
/usr/bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic openaq-country-data
/usr/bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic openaq-parameter-data
/usr/bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic openaq-locations-data
/usr/bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 3 --topic openaq-latest-measurements
