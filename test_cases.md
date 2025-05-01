##  Test Case 1: Check the postgres container 
**Description:** Ensure the PostgreSQL service starts and connects to the right  user and database.

| Step | Action                    | Expected/Actual Outcome        
|------|---------------------------|-----------------------------
| 1    | Run `docker-compose up`   | Container starts        
| 2    | Connect via `psql`        | Able to log in with `proj4streamdata/pass_word`   
| 3    | Run `\l`                  |`realtimedata` DB exists   
| 4    | Run `\du`                 | Role `proj4streamdata` exists 

---

##  Test Case 2: Spark Submits Job Successfully

**Description:** Confirm Spark container starts and submits `spark_streaming_to_postgres.py`.

| Step | Action                       | Expected/Actual Outcome 
|------|------------------------------|---------------------------
| 1    | Start containers             | Spark logs show submission of script   
| 2    | Check `spark` container logs | confirm spark  job is running and waiting for data   

---

##  Test Case 3: Data Streames from CSV to Postgres

**Description:** Check if placing a CSV into `data/` triggers ingestion and inserts into Postgres.

| Step | Action                         | Expected/Actual Outcome 
|------|--------------------------------|---------------------------------
| 1    | Generate CSV in `data/` folder | Spark detects and reads the file   
| 2    | Query Postgres table           | Data from CSV appears in target table   

---

##  Test Case 4: Error Handling – Missing CSV Schema

**Description:** Check Spark behavior when schema is missing or invalid.

| Step | Action                           | Expected/Actual Outcome
|------|----------------------------------|-----------------------------
| 1 | Remove schema code from Spark script| Spark throws a schema error   

---


