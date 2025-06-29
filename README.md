# NoSQL-Datawarehouse-Project
A comprehensive end-to-end data engineering project using Apache Cassandra, Google Cloud Platform, and modern data tools


## Project Structure
NoSQL-Datawarehouse-Project/
├── config/                   # Configuration files
├── docs/                     # Project documentation
├── docker/                   # Docker configurations
│   ├── airflow/              # Airflow setup
│   ├── kafka/                # Kafka configuration
│   ├── cassandra/            # Cassandra setup
│   └── spark/                # Spark configuration
├── dags/                     # Airflow DAGs
├── dbt/                      # dbt project
│   ├── models/               # dbt models
│   ├── tests/                # Data quality tests
│   └── docs/                 # Documentation
├── scripts/                  # Utility scripts
├── src/                      # Source code
│   ├── data_generation/      # Faker data generators
│   ├── kafka_producers/      # Kafka producers
│   ├── spark_jobs/           # Spark processing jobs
│   └── utils/                # Utility functions
