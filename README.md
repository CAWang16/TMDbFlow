# TMDbFlow ETL Pipeline - README

Welcome to **TMDbFlow**, an ETL pipeline designed to automate the extraction, transformation, and loading of movie data from The Movie Database (TMDb) API. This README will guide you through setting up, running, and expanding the project.

---

## **Prerequisites**
Ensure you have the following installed:
- **Docker & Docker Compose** – For containerized Airflow and other services.
- **Python 3.8+** – For API scripts and data transformation.
- **Airflow** (Optional) – For DAG automation.
- **PostgreSQL/MySQL** (Optional) – For loading data into a database.

---

## **Project Setup**

### 1. Clone the Repository
```bash
git clone https://github.com/CAWang16/TMDbFlow.git
cd TMDbFlow
```

### 2. Configure Environment Variables
Create a `.env` file in the root directory with the following content:

```bash
TMDB_API_KEY=your_tmdb_api_key
```
Obtain your API key from TMDb.

### 3. Start Services
Run Docker containers for Airflow (if needed):

```bash
docker-compose up -d
```
`-d` runs containers in detached mode.
Ensure Airflow is accessible at [http://localhost:8081](http://localhost:8081).

---

## **Running the Pipeline**

### 1. Extract Data
Run the extraction script to fetch movie data from TMDb:

```bash
python airbyte/my_custom_source/dags/Extract_data.py
```
This script will save raw JSON data to `raw_movies.json`.

### 2. Transform Data
Transform the raw JSON into a clean, structured format:

```bash
python airbyte/my_custom_source/dags/Transform_data.py
```
Cleaned data will be saved to `transformed_movies.csv`.
A separate file `normalized_genres.csv` stores normalized genre data.

### 3. Load Data (Optional)
If loading to a SQL database:

```bash
python airbyte/my_custom_source/dags/Load_data.py
```
Ensure database credentials are configured in `load_to_db.py`.

---

## **Automating with Airflow(The Following Step)**

Place the DAG file in `airflow/dags/`.
Restart Airflow to detect new DAGs:

```bash
docker-compose restart airflow-webserver
```

Trigger the DAG through the Airflow UI at:

```bash
http://localhost:8080
```

## **Difficulties Encountered and Solutions**

### PostgreSQL Connection Issues:
**Issue:** PostgreSQL refused connections on localhost:5432.
**Solution:**
To find the correct connection details, run:

```bash
docker inspect airflow_postgres
```

Update the pgAdmin connection to use the container’s IP or ensure PostgreSQL listens on `0.0.0.0` by modifying `postgresql.conf` and `pg_hba.conf`.

---

### Log Files Preventing Git Commit (Invalid argument):
**Issue:** Git failed to add files in the `logs/scheduler/latest` directory.
**Solution:**
Add the following to `.gitignore`:

```bash
airbyte/my_custom_source/logs/
```

Clear cached logs if already tracked:

```bash
git rm --cached -r airbyte/my_custom_source/logs/
```

---

### Editing PostgreSQL Config Files (pg_hba.conf, postgresql.conf):
**Issue:** `vi` and `nano` were missing from the Docker container.
**Solution:**
Copy the configuration files to the host, edit them, and copy them back to the container:

```bash
docker cp airflow_postgres:/var/lib/postgresql/data/pg_hba.conf ./pg_hba.conf
notepad ./pg_hba.conf  # Edit to allow external connections
docker cp ./pg_hba.conf airflow_postgres:/var/lib/postgresql/data/pg_hba.conf
docker restart airflow_postgres
```

---

### Mapping PostgreSQL to the Correct Port:
**Issue:** PostgreSQL initially exposed port 5433, which caused pgAdmin connection issues.
**Solution:**
Update `docker-compose.yml` to map to port 5432:

```yaml
ports:
  - "5432:5432"
```
