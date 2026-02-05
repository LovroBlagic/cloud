Reddit Data Streaming & Analytics Pipeline

This project implements an end-to-end cloud data pipeline that streams Reddit posts through Google Pub/Sub, processes them with Dockerized Python services on Cloud Run, and stores analytics-ready data in BigQuery. The pipeline uses schema validation, dead-letter topics, dbt medallion architecture, and Great Expectations to ensure data quality, with automated CI/CD and dashboards built in Looker Studio.

Project Overview

A cloud-native data streaming and analytics platform built on Google Cloud.

A Python producer fetches posts from the Reddit API and publishes messages to Pub/Sub. A consumer service validates and processes incoming data, persists it to Google Cloud Storage (Parquet/JSON), and loads it into BigQuery. Downstream transformations are handled with dbt using a bronze/silver/gold (medallion) architecture, data quality is enforced with Great Expectations, and results are visualized in Looker Studio. The entire workflow is automated using GitHub Actions CI/CD.

Architecture

Reddit API  
→ Python Producer (Cloud Run Job)  
→ Pub/Sub (Schema Registry + Dead-Letter Topic)  
→ Python Consumer (Cloud Run Service)  
→ Google Cloud Storage (Parquet / JSON)  
→ BigQuery  
→ dbt (Bronze / Silver / Gold models)  
→ Great Expectations (Data Quality)  
→ Looker Studio Dashboard

---

Features

- Real-time ingestion of Reddit data
- Pub/Sub with Schema Registry and Dead-Letter Topic for message validation
- Dockerized producer & consumer deployed on Cloud Run
- Storage of raw and processed data in GCS (Parquet + JSON)
- Automated loading into BigQuery for analytics
- Medallion architecture (bronze / silver / gold) implemented with dbt
- Data quality checks using Great Expectations
- Trend & sentiment aggregations with SQL window functions
- CI/CD pipeline using GitHub Actions
- Interactive dashboard in Looker Studio

Implemented with

- Python
- Google Cloud Platform (Pub/Sub, Cloud Run, GCS, BigQuery)
- Docker
- dbt
- Great Expectations
- GitHub Actions (CI/CD)
- Looker Studio
