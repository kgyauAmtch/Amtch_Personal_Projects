## Test Case ID: #001
Test Scenario: To authenticate the generation of data in the folder and 
Test Steps:
The user navigates to Gmail.com.
The user enters a registered email address in the ’email’ field.
The user clicks the ‘Next’ button.
The user enters the registered password.
The user clicks ‘Sign In.’
Prerequisites: A registered Gmail ID with a unique username and password.
Browser: Chrome v 86. Device: Samsung Galaxy Tab S7.
Test Data: Legitimate username and password.
Expected/Intended Results: Once username and password are entered, the web page redirects to the user’s inbox, displaying and highlighting new emails at the top.
Actual Results: As Expected
Test Status – Pass/Fail: Pass

#  Manual Test Plan for Spark to PostgreSQL Streaming

This document outlines manual test cases for validating the Spark streaming pipeline into PostgreSQL.

---

##  Test Case 1: Postgres Container Starts with Correct Config

**Description:** Ensure the PostgreSQL service starts and exposes the correct user/database.

| Step | Action | Expected Outcome | Actual Outcome | Pass/Fail |
|------|--------|------------------|----------------|-----------|
| 1 | Run `docker-compose up` | Container starts |  |  |
| 2 | Connect via `psql` | Able to log in with `proj4streamdata/pass_word` |  |  |
| 3 | Run `\l` | `realtimedata` DB exists |  |  |
| 4 | Run `\du` | Role `proj4streamdata` exists |  |  |

---

##  Test Case 2: Spark Submits Job Successfully

**Description:** Confirm Spark container starts and submits `spark_streaming_to_postgres.py`.

| Step | Action | Expected Outcome | Actual Outcome | Pass/Fail |
|------|--------|------------------|----------------|-----------|
| 1 | Start containers | Spark logs show submission of script |  |  |
| 2 | Check `spark` container logs | Job running and waiting for data |  |  |

---

##  Test Case 3: Data Streamed from CSV to Postgres

**Description:** Check if placing a CSV into `data/` triggers ingestion and inserts into Postgres.

| Step | Action | Expected Outcome | Actual Outcome | Pass/Fail |
|------|--------|------------------|----------------|-----------|
| 1 | Place CSV in `data/` folder | Spark detects and reads the file |  |  |
| 2 | Query Postgres table | Data from CSV appears in target table |  |  |

---

##  Test Case 4: Error Handling – Missing CSV Schema

**Description:** Check Spark behavior when schema is missing or invalid.

| Step | Action | Expected Outcome | Actual Outcome | Pass/Fail |
|------|--------|------------------|----------------|-----------|
| 1 | Remove schema code from Spark script | Spark throws a clear schema error |  |  |

---

## Test Case 5: Clean Shutdown and Restart

**Description:** Verify cleanup of services and data when restarting from scratch.

| Step | Action | Expected Outcome | Actual Outcome | Pass/Fail |
|------|--------|------------------|----------------|-----------|
| 1 | Run `docker-compose down -v` | All services and volumes stopped/removed |  |  |
| 2 | Run `docker-compose up` again | Services start fresh; data reinitialized |  |  |

---

## Notes

- Add more edge case tests (e.g., malformed CSV, DB connection error).
- Validate performance if needed (batch latency, memory use).
- Logs should be monitored continuously to catch silent failures.

---

