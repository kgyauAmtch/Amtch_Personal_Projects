# Project Overview: Real-Time Spark to PostgreSQL Streaming Pipeline

This document provides a brief overview of the system's components and how they interact to enable real-time data ingestion into a PostgreSQL database using Apache Spark.

---

## Components

### 1. **PostgreSQL (Docker Container)**
- Acts as the storage layer for the processed streaming data.
- Initialized with a default database `realtimedata` and user `proj4streamdata`.
- Listens on port `5432`.

### 2. **Apache Spark (Docker Container)**
- Runs a `spark-submit` job that reads streaming data from a local `data/` directory.
- Uses the Spark Structured Streaming API.
- Submits the job via `spark_streaming_to_postgres.py`.
- Includes JDBC driver (`postgresql-42.7.5.jar`) to connect to the database.

### 3. **Docker Compose**
- Orchestrates the PostgreSQL and Spark containers.
- Ensures Spark waits until PostgreSQL is healthy before starting.

### 4. ** Data Directory** (`./data/`)
- stores generated data form the data_creaation.py
- Holds incoming CSV files which are picked up by Spark for ingestion.
- Simulates a data stream by appending new files.

---

## System Flow

```text
CSV File → Spark Container (readStream) → Transformations → PostgreSQL Table
```

1. **Generated CSV files every two seconds** into `./data/` folder.
2. **Spark detects new files** using `readStream` and applies the defined schema.
3. **Data is transformed**  and  **written via JDBC** to PostgreSQL.
4. **PostgreSQL stores the records** in a target table (configured in the script).

---



