# Real-Time Data Pipeline: MySQL to PostgreSQL via Confluent Kafka

This project demonstrates an end-to-end real-time data pipeline where data is streamed from MySQL into PostgreSQL using Confluent Kafka and Python producer/consumer scripts.

The setup includes:
- **MySQL Workbench** for managing the source database
- **PostgreSQL** with **DBeaver** for monitoring the target database
- **Confluent Kafka** as the messaging backbone for real-time event streaming
- **Python producer and consumer scripts** for publishing and processing records through Kafka topics

## Project Overview

In this demo, the full pipeline flow is showcased:

1. Insert or update data in MySQL
2. Publish events into Kafka topics
3. Consume events in real time using Python
4. Load processed data into PostgreSQL

The goal of the project was to simulate a lightweight real-time ETL/data integration pipeline commonly used in modern data engineering architectures.

## Technologies Used

- MySQL
- PostgreSQL
- Confluent Kafka
- Python
- DBeaver
- MySQL Workbench
