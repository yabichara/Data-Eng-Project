# OpenAQ Data Engineering Project

This project is a robust pipeline for extracting, processing, and storing air quality data from the [OpenAQ API](https://docs.openaq.org/). It integrates real-time data processing, batch processing, and streaming using cutting-edge technologies like Kafka, Spark, PostgreSQL, and Airflow.

## Key Features
1. **Data Extraction**: Fetches air quality data (parameters, locations, and countries) from the OpenAQ API.
2. **Kafka Streaming**: Utilizes Kafka for streaming data between components.
3. **Batch Processing**: Processes dimension data with Apache Spark.
4. **Real-Time Streaming**: Processes real-time measurements using Spark Streaming.
5. **Data Storage**: Stores processed data in PostgreSQL.
6. **Orchestration and Monitoring**: Uses Airflow to manage tasks and monitor pipelines.

---

## Project Architecture

The pipeline consists of the following steps:
1. **Data Fetching**: Extracts data from the OpenAQ API.
2. **Data Streaming**: Publishes the extracted data to Kafka topics.
3. **Data Processing**: Uses Spark to process and enrich the data.
4. **Data Storage**: Writes processed data to PostgreSQL.
5. **Real-Time Streaming**: Uses Spark Streaming for real-time measurements.

![Project Architecture](img/Project_architecture.jpeg)

---

## Prerequisites

Before running the project, ensure you have:
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

---

## Project Structure

---

## Getting Started

### 1. Clone the Repository

```bash
git clone https://github.com/Mehdi-24-K4/DataEngProject.git
cd DataEngProject
```

### 2. Set Up API Key for OpenAQ
Create a file named ```api_key.json``` in the ```dags``` directory to store your OpenAQ API key:
```json
{
  "New_API_KEY": "your_openaq_api_key"
}
```

### 3. Build and Start Docker Services
Run the following command to build and start the services:
```bash
docker-compose up -d --build
```
The following services will be deployed:
- Airflow: For orchestrating the pipeline.
- Kafka: For streaming data between components.
- PostgreSQL: For storing processed data.
- Spark: For processing batch and real-time data.
- Supervisor: For monitoring DAG progress and triggering Spark Streaming.

### 4. Access Airflow UI
- Open your browser and go to ```http://localhost:8080```.
- Log in with the default credentials:
  - Username: ```airflow```
  - Password: ```airflow```.
  
Enable and trigger the DAG named ```initialize_pipeline_dag```.

### 5. Monitor Spark Streaming
Once the DAG completes processing the dimensions, the Supervisor container detects when the last task begins and automatically triggers the Spark Streaming job. This job processes real-time measurements from Kafka and stores them in PostgreSQL.

## 🛠 How It Works

### 1. Batch Processing (Airflow DAG)
- Step 1: Extract data from the OpenAQ API using the ```initialize_pipeline_dag.py```.
- Step 2: Process the data with Spark and store it in PostgreSQL via temporary tables.
- Step 3: Kafka topics are used as intermediaries for dimension data (parameters, stations, country).
### 2. Real-Time Processing (Spark Streaming)
- Step 1: Supervisor container detects the final task in the DAG.
- Step 2: Triggers Spark Streaming (```spark_stream.py```) to process the latest measurement data.
- Step 3: Data is stored in PostgreSQL in near real-time.
<!-- 
## Technologies Utilisées

- **Apache Kafka** : Broker de messages distribué pour la gestion des flux de données.
- **Zookeeper** : Service de coordination pour Kafka.
- **MongoDB** : Base de données NoSQL utilisée comme stockage temporaire des données non transformées.
- **Apache Spark** : Framework de traitement de données en cluster utilisé pour les étapes de transformation et d'agrégation des données. Spark permet d'accélérer le traitement en distribuant les calculs sur plusieurs partitions.
- **PostgreSQL** : Base de données relationnelle pour le stockage final des données sous un schéma en étoile, facilitant les requêtes analytiques.
- **Airflow** : Orchestrateur de workflows pour la gestion des différentes tâches ETL.
- **Docker et Docker Compose** : Pour la conteneurisation et l'isolation des services, facilitant le déploiement et la gestion des dépendances.
- **Grafana** : Pour la visualisation des données collectées, sous forme de tableaux de bord dynamiques.

## Schéma en Étoile

Le schéma en étoile est conçu pour faciliter les analyses OLAP (Online Analytical Processing). Il est composé de :

- **Tables de dimension** :
  - `dimension_location` : stocke les informations de localisation (latitude, longitude).
  - `dimension_parameter` : stocke les paramètres surveillés (comme PM2.5, NO2, etc.).
  - `dimension_time` : stocke les informations temporelles (timestamp).
  
- **Table de faits** :
  - `air_quality_measurements` : contient les mesures de la qualité de l'air, avec des clés étrangères vers les dimensions `location`, `parameter`, et `time`.

## Prérequis

- [Docker](https://www.docker.com/) et [Docker Compose](https://docs.docker.com/compose/)
- Python 3.x
- Accès à l'API OpenAQ (une clé API peut être requise selon les conditions d'accès)

## Installation

1. **Clonez le dépôt** :
    ```bash
    git clone https://github.com/username/repository.git
    cd repository
    ```

2. **Démarrez les conteneurs Docker** :
    Utilisez Docker Compose pour démarrer tous les services nécessaires (Kafka, Zookeeper, MongoDB, PostgreSQL, Airflow, Spark, etc.) :
    ```bash
    docker-compose up --build
    ```

3. **Accédez à l'interface Airflow** :
    Une fois les conteneurs démarrés, Airflow sera disponible à l'adresse [http://localhost:8084](http://localhost:8084). Vous pouvez y gérer et surveiller l'exécution du pipeline.

## Utilisation

### 1. Extraction des données
Le pipeline ETL est orchestré avec Apache Airflow. Le DAG `air_quality_pipeline` est configuré pour s'exécuter toutes les 10 minutes, ce qui signifie qu'il collectera automatiquement de nouvelles données depuis l'API OpenAQ, les enverra à Kafka, puis les transformera et les chargera dans PostgreSQL.

### 2. Orchestration des tâches
Le pipeline est composé de trois tâches principales :
- **Extraction** : `produce_air_quality_data` — Cette tâche interagit avec l'API OpenAQ pour extraire les données et les envoyer à Kafka.
- **Consommation** : `consume_and_store_data` — Les données extraites sont consommées par Kafka et stockées dans MongoDB.
- **Transformation et Chargement** : `transform_store_postgreSQL` — À l'aide de Spark, les données sont nettoyées, les tables de dimensions et de faits sont créées, et les données sont chargées dans PostgreSQL.

Vous pouvez surveiller et gérer ces tâches via l'interface Airflow.

### 3. Visualisation des données
Une fois les données chargées dans PostgreSQL, vous pouvez les visualiser et les analyser avec Grafana. Un tableau de bord Grafana peut être configuré pour suivre les tendances des mesures de qualité de l'air au fil du temps.

## Configuration du DAG dans Airflow

Le DAG `air_quality_pipeline` se trouve dans le fichier `dags/air_quality_pipeline.py`. Il est configuré pour s'exécuter toutes les 10 minutes et suit cette séquence :

1. Extraction des données depuis OpenAQ avec la tâche `produce_air_quality_data`.
2. Consommation des données Kafka avec `consume_and_store_data` et stockage temporaire dans MongoDB.
3. Transformation des données et chargement dans PostgreSQL avec `transform_store_postgreSQL`.

## Optimisation avec Apache Spark

Le traitement des données dans ce pipeline est optimisé grâce à Apache Spark :
- Spark permet de gérer efficacement des volumes importants de données en distribuant les calculs sur plusieurs partitions.
- Les données sont d'abord transformées dans des tables de dimensions et de faits. Les dimensions `location`, `parameter`, et `time` sont dédupliquées et attribuées des ID uniques.
- Le pipeline utilise Spark SQL pour appliquer les transformations complexes et les jointures nécessaires.
- La coalescence des partitions dans Spark permet de réduire le nombre de partitions avant le chargement dans PostgreSQL, optimisant ainsi l'insertion des données.

## Surveillance et Debugging

- **Airflow** : Airflow permet de suivre le statut des tâches en temps réel et de voir les logs détaillés en cas d'erreurs.
- **Logs Spark** : Les logs des tâches Spark peuvent être consultés via les journaux Airflow ou directement dans le conteneur Spark.
- **Base de données PostgreSQL** : Vous pouvez accéder à la base PostgreSQL pour vérifier que les données ont bien été chargées.

## Conclusion

Ce projet met en place un pipeline ETL robuste et scalable pour collecter, transformer et analyser les données de la qualité de l'air. Grâce à des outils comme Kafka, Spark, et PostgreSQL, il est capable de gérer efficacement des volumes importants de données tout en offrant des possibilités d'analyse via un schéma en étoile.

---

## Auteur

*Ouazzani Jamil Mehdi* - [Votre profil GitHub](https://github.com/Mehdi-24-K4) -->