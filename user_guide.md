## Instructions to run the project

This guide explains how to set up and run the Spark + PostgreSQL streaming pipeline using Docker.

---

##  Prerequisites

Before you begin, make sure you have the following installed:

- Docker
- python and spark 

---

## Project Folder Structure

```
project-root/
│
├── docker-compose.yml
├── src/
│   ├── spark_streaming_to_postgres.py
│   ├── postgresql-42.7.5.jar
│   ├── schema.py
│   ├── spark_streaming_to_postgres.py
│   ├── data_generator.py
│   └── helper_functions.py
├── db/
│   |- postgres_connection_details.txt
│   └── postgres_setup.sql 
└── data/
    └── (CSV files for streaming go here)
```

---

## How to Run the Project

### 1. Genrate CSV Files to Stream

Run the data_generator script to simulate incoming data into the `./data/` directory. Spark will automatically pick them up and begin processing.

---

### 2. Clean Start (Recommended for First Run)

Remove any previously created containers and volumes:
-  using this line 
```
docker-compose down -v
```

---

### 3. Build and Start the Services

- Run the containers(basically running the docker-compose file):

```
docker-compose up --build
```

This will:

- Start PostgreSQL (`db`) and initialize the database
- Run your Spark job to stream data from `data/` into Postgres database 

---

### 4. Verify Data in PostgreSQL

You can connect to the database using any Postgres client (e.g. `psql`, DBeaver, PgAdmin):

```bash
psql -h localhost -p 5433 -U proj4streamdata -d realtimedata
```

> Password: `pass_word`

Run a query:

```sql
SELECT * FROM your_table_name;
```

---

##  Stop the Project

To stop and remove containers:

```bash
docker-compose down
```

To remove volumes (erases the database):

```bash
docker-compose down -v
```

---

##  Notes

- Spark logs are available in the `spark` container output.
- The JDBC `.jar` file must be present in the `src/` directory.
- Modify the table name or schema in `spark_streaming_to_postgres.py` as needed.

---

