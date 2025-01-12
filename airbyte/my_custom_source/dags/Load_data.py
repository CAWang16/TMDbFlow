import psycopg2
import json
from pathlib import Path
from datetime import datetime

DATA_FOLDER = Path("./airbyte/my_custom_source/data")
# Get the absolute path to the 'data' folder
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # Adjust the path relative to your script location
DATA_FOLDER = BASE_DIR / "my_custom_source" / "data"

# Ensure the folder path is correct
print(f"Data folder path: {DATA_FOLDER}")

# Database connection details
db_params = {
    "dbname": "airflow",
    "user": "airflow",
    "password": "airflow",
    "host": "localhost",
    "port": "5432"
}

# Table creation queries
queries = {
    "popular_movies": """
    CREATE TABLE IF NOT EXISTS popular_movies (
        id SERIAL PRIMARY KEY,
        movie_id INT UNIQUE,
        title TEXT NOT NULL,
        release_date DATE,
        popularity FLOAT,
        overview TEXT,
        vote_average FLOAT,
        vote_count INT,
        genre_ids JSONB
    );
    """,
    "top_rated_movies": """
    CREATE TABLE IF NOT EXISTS top_rated_movies (
        id SERIAL PRIMARY KEY,
        movie_id INT UNIQUE,
        title TEXT NOT NULL,
        release_date DATE,
        popularity FLOAT,
        overview TEXT,
        vote_average FLOAT,
        vote_count INT,
        genre_ids JSONB
    );
    """,
    "upcoming_movies": """
    CREATE TABLE IF NOT EXISTS upcoming_movies (
        id SERIAL PRIMARY KEY,
        movie_id INT UNIQUE,
        title TEXT NOT NULL,
        release_date DATE,
        popularity FLOAT,
        overview TEXT,
        vote_average FLOAT,
        vote_count INT,
        genre_ids JSONB
    );
    """
}

# Function to create tables
def create_tables():
    try:
        # Connect to the database
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        # Execute each table creation query
        for table_name, query in queries.items():
            cursor.execute(query)
            print(f"Table '{table_name}' is ready (created if it didn't exist).")
        
        # Commit changes and close connection
        conn.commit()
        cursor.close()
        conn.close()
        print("All tables are set up successfully!")
    except Exception as e:
        print(f"Error while creating tables: {e}")
def load_data_into_table(source_name, table_name):
    try:
        file_path = DATA_FOLDER / f"{source_name}"

        # Check if the file exists
        if not file_path.exists():
            print(f"File not found: {file_path}")
            return
        
        conn = psycopg2.connect(**db_params)
        cursor = conn.cursor()
        
        try:
            with file_path.open("r", encoding="utf-8") as f:
                # Load the JSON content
                data = json.load(f)
                if not data:  # Check if the file is empty (e.g., `[]`)
                    print(f"No data found in {file_path}, skipping.")
                    return
        except json.JSONDecodeError as e:
            print(f"Invalid JSON format in {file_path}: {e}")
            return
        
        records = [
            (
                item["id"],
                item["title"] if item["title"] else None,
                item["release_date"] if item["release_date"] else None,
                item["popularity"] if item["popularity"] else None,
                item["overview"] if item["overview"] else None,
                item["vote_average"] if item["vote_average"] else None,
                item["vote_count"] if item["vote_count"] else None,
                json.dumps(item["genre_ids"])
            )
            for item in data
        ]
        
        # Insert data into table
        query = f"""
        INSERT INTO {table_name} (movie_id, title, release_date, popularity, overview, vote_average, vote_count, genre_ids)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (movie_id) DO NOTHING; -- Avoid duplicates
        """
        
        cursor.executemany(query, records)
        conn.commit()
        
        print(f"Loaded {len(records)} records into '{table_name}'.")
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"Error while loading data into '{table_name}': {e}")

# Call the function
if __name__ == "__main__":
    # Step 1: Create tables
    create_tables()
    current_date = datetime.now().strftime("%Y-%m-%d")

    # Step 2: Load data into tables
    data_files = {
        "popular_movies": f"popular_movies_{current_date}.json",
        "top_rated_movies": f"top_rated_movies_{current_date}.json",
        "upcoming_movies": f"upcoming_movies_{current_date}.json"
    }
    
    for table_name, source_name in data_files.items():
        print(f"Loading data into '{table_name}' from '{source_name}'...")
        load_data_into_table(source_name, table_name)