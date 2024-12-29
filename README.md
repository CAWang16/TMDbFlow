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
Ensure Airflow is accessible at [http://localhost:8080](http://localhost:8080).

---

## **Running the Pipeline**

### 1. Extract Data
Run the extraction script to fetch movie data from TMDb:

```bash
python src/extract/source.py
```
This script will save raw JSON data to `raw_movies.json`.

### 2. Transform Data
Transform the raw JSON into a clean, structured format:

```bash
python src/transform/transform.py
```
Cleaned data will be saved to `transformed_movies.csv`.
A separate file `normalized_genres.csv` stores normalized genre data.

### 3. Load Data (Optional)
If loading to a SQL database:

```bash
python src/load/load_to_db.py
```
Ensure database credentials are configured in `load_to_db.py`.

---

## **Automating with Airflow**

Place the DAG file in `airflow/dags/`.
Restart Airflow to detect new DAGs:

```bash
docker-compose restart airflow-webserver
```

Trigger the DAG through the Airflow UI at:

```bash
http://localhost:8080
```