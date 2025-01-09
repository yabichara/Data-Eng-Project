# Projet ETL pour l'Analyse des Données de la Qualité de l'Air en temps réel

Ce projet met en place un pipeline ETL (Extract, Transform, Load) pour l'analyse des données de qualité de l'air en temps réel en utilisant l'API OpenAQ. Les données sont extraites, transformées et chargées dans une base de données PostgreSQL avec un schéma en étoile optimisé pour l'analyse. Le pipeline est orchestré via Airflow, et le traitement des données est accéléré grâce à Apache Spark.

## Description

Le projet est composé de plusieurs scripts Python et utilise Apache Airflow pour orchestrer les différentes étapes du pipeline ETL. Le pipeline fonctionne comme suit :

1. **Extraction des données** : Les données de qualité de l'air sont extraites de l'API OpenAQ et envoyées à Kafka, un broker de messages distribué.
2. **Consommation des données** : Les données sont consommées depuis Kafka et stockées temporairement dans une base MongoDB NoSQL.
3. **Transformation des données** : À l'aide d'Apache Spark, les données sont nettoyées, transformées, et des tables de dimension (location, time, parameter) ainsi qu'une table de faits sont créées selon un schéma en étoile.
4. **Chargement des données** : Les tables dimensionnelles et factuelles sont ensuite chargées dans PostgreSQL pour une analyse efficace.
5. **Orchestration** : Le processus global est orchestré par Airflow avec des tâches planifiées pour s'exécuter périodiquement (toutes les 10 minutes).

### Architecture de la Pipeline

Voici un schéma illustrant le flux de données et les interactions entre les différentes composantes du système :

![Schéma de la Pipeline](img/Project_architecture.jpeg)

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

*Ouazzani Jamil Mehdi* - [Votre profil GitHub](https://github.com/Mehdi-24-K4)