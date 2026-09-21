# DataForge — Event-Driven Data Platform with CDC & Data Contracts

## Business Context

NordMarket GmbH is a fictional European e-commerce company that needs reliable
near-real-time data pipelines for analytics and operational monitoring.

## Project Objective

DataForge is an event-driven data platform that captures database changes
using Change Data Capture (CDC), streams events through Kafka, validates
events using data contracts, processes streaming data with PySpark, and
delivers trusted analytical data to a cloud data lake and warehouse.

## Architecture

PostgreSQL
→ Debezium
→ Kafka
→ Schema Registry
→ Data Contracts
→ PySpark
→ Azure Data Lake
→ Data Warehouse
→ dbt
→ Power BI

## Technologies

- Python
- PostgreSQL
- Debezium
- Apache Kafka
- Schema Registry
- PySpark
- Azure Data Lake Gen2
- SQL
- dbt
- Apache Airflow
- Docker
- GitHub Actions
- Power BI