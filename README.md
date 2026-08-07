# 🚀 MongoDB to Delta Lake Data Pipeline

## 📌 Overview

This project demonstrates an end-to-end data ingestion and
transformation pipeline.

Data is extracted from MongoDB, processed using PySpark, and stored in
Delta Lake using a layered architecture (Bronze > Silver > Gold).

---

## 🛠️ Tech Stack

- PySpark -- Distributed data processing
- Delta Lake -- Data lake storage with ACID properties
- Apache Airflow -- Workflow orchestration
- MongoDB -- Source database

---

## ⚙️ Pipeline Architecture

MongoDB > PySpark > Bronze (Raw) > Silver (Cleaned) > Gold (Analytics)

- Bronze Layer: Raw data ingestion from MongoDB
- Silver Layer: Data cleaning and transformation
- Gold Layer: Final datasets ready for analysis

---

## ✨ Features

- MongoDB data ingestion using PySpark
- Schema-based and schema-inferred loading
- Delta Lake storage with structured layers
- Full load ingestion pipeline
- Airflow DAG-based orchestration

---

## 📁 Project Structure

- ├── dags/ # Airflow DAGs for orchestration
- ├── jobs/ # PySpark ETL jobs
- ├── schema/ # Schema definitions
- ├── data/ # Output Delta tables (bronze/silver/gold)

---

## ▶️ Pipeline Flow

1.  Extract data from MongoDB
2.  Load raw data into Bronze layer
3.  Transform and clean data into Silver layer
4.  Prepare analytics-ready data in Gold layer

---

## 📊 Use Cases

- Building data lake pipelines
- ETL/ELT workflows
- Batch data processing
- Data engineering project implementation

---

## 🚧 Future Improvements

- Data validation and quality checks
- Logging and monitoring enhancements
- Advanced transformations in Gold layer

---

## 👨‍💻 Author

Viraj\
Data Engineering Enthusiast 🚀
