# MongoDB to Delta Lake Pipeline (Local Setup)

## Overview

This project demonstrates data ingetion pipeline using:

- PySpark
- Delta Lake
- Apache Airflow

## Features

- Ingestion from MongoDB
- Schema-based or inferred loading
- Bronze layer Delta tables
- Airflow orchestration

## Project Structure

- dags/ → Airflow DAGs
- jobs/ → PySpark jobs
- schemas/ → Optional schemas
- data/ → Delta output
