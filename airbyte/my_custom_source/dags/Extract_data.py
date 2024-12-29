import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.models import AirbyteStream, SyncMode, AirbyteRecordMessage
from airbyte_cdk.sources.streams.http import HttpStream
from datetime import datetime
from typing import Iterable, Any, Mapping, Optional
from dotenv import load_dotenv
from pathlib import Path
import os
import csv
import time
import json

# ---- Load Environment Variables ----
load_dotenv()

api_key = os.getenv("TMDB_API_KEY")

if not api_key:
    raise ValueError("TMDB API key not found. Please check your .env file.")

config = {
    "api_key": api_key
}

# ---- Source Class ----
class SourceTmdbApi(AbstractSource):
    # To check the API is available 
    def check_connection(self, config) -> tuple[bool, any]:
        url = "https://api.themoviedb.org/3/movie/popular"
        headers = {"Authorization": f"Bearer {config['api_key']}"}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, f"Connection failed: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> list:
        movie_ids = self.get_movie_ids(config)  # Fetch IDs before passing them to the streams

        return [
            TmdbStream(config=config, endpoint="movie/popular", name="popular_movies"),
            TmdbStream(config=config, endpoint="movie/top_rated", name="top_rated_movies"),
            TmdbStream(config=config, endpoint="movie/upcoming", name="upcoming_movies"),
            TmdbStream(config=config, endpoint="genre/movie/list", name="movie_genres"),
            *(TmdbStream(config=config, endpoint=f"movie/{movie_id}/credits", name=f"credits_{movie_id}") for movie_id in movie_ids),
        ]
    def get_movie_ids(self, config: Mapping[str, Any]) -> list:
        endpoints = ["popular", "top_rated", "upcoming"]
        movie_ids = []
        headers = {"Authorization": f"Bearer {config['api_key']}"}

        for endpoint in endpoints:
            url = f"https://api.themoviedb.org/3/movie/{endpoint}"
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            data = response.json()
            movie_ids.extend(movie["id"] for movie in data.get("results", []))
        
        return movie_ids
    
# ---- Stream Class ----
class TmdbStream(HttpStream):
    url_base = "https://api.themoviedb.org/3/"
    primary_key = "id"
    last_fetched_date = None  # Moved inside the class to avoid global variable use

    def __init__(self, config: Mapping[str, Any], endpoint: str, name: str):
        super().__init__()
        self.api_key = config["api_key"]
        self.endpoint = endpoint
        self.name = name

    def path(self, **kwargs) -> str:
        return self.endpoint

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json()
        current_page = data.get("page", 1)
        total_pages = data.get("total_pages", 1)

        if current_page < min(total_pages, 10):  # Cap pages at 10
            return {"page": current_page + 1}
        return None

    def handle_rate_limit(self, response):
        if response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))
            print(f"Rate limited. Retrying in {retry_after} seconds...")
            time.sleep(retry_after)
            return True
        return False

    def request_params(self, stream_state: Mapping[str, Any], stream_slice: Mapping[str, Any] = None,
                       next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]:
        params = {"page": next_page_token["page"] if next_page_token else 1}

        # Incremental data extraction filter
        if self.last_fetched_date:
            params["release_date.gte"] = self.last_fetched_date

        return params

    def request_headers(self, **kwargs) -> Mapping[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:

        if response.status_code == 404:
            print(f"Credits not found for endpoint: {response.url}")
            return  # Skip and continue
        
        if self.handle_rate_limit(response):
            response = requests.get(response.url, headers=self.request_headers())
            response.raise_for_status()  # Ensure the new response is valid

        data = response.json()
        results = data.get("results", [])

        # Track latest date after parsing all results
        latest_date = self.last_fetched_date

        for movie in data.get("results", []):
            yield {
                "id": movie.get("id"),
                "title": movie.get("title"),
                "release_date": movie.get("release_date"),
                "popularity": movie.get("popularity"),
                "overview": movie.get("overview"),
                "vote_average": movie.get("vote_average"),
                "vote_count": movie.get("vote_count"),
                "genre_ids": movie.get("genre_ids"),
            }
            # Update last_fetched_date
            if movie.get("release_date"):
                latest_date = max(latest_date or movie.get("release_date"), movie.get("release_date"))

        # Update class-level last_fetched_date
        self.last_fetched_date = latest_date

    def read_records(self, sync_mode: SyncMode, stream_slice: Mapping[str, Any] = None,
                     stream_state: Mapping[str, Any] = None) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(sync_mode, stream_slice, stream_state):
            yield AirbyteRecordMessage(
                stream=self.name,
                data=record,
                emitted_at=int(datetime.now().timestamp()) * 1000
            )
# ---- File Handling for Incremental Extraction ----
def load_last_fetched_date() -> Optional[str]:
    if os.path.exists("last_date.txt"):
        with open("last_date.txt", "r") as f:
            return f.read().strip()
    return None

def save_last_fetched_date(date: str):
    with open("last_date.txt", "w") as f:
        f.write(date)

# ---- CSV Save Function ----
def save_to_csv(records: list, output_file: str):
    if not records:
        print("No new records to save.")
        return
    
    clean_records = [record.data for record in records]
    headers_written = os.path.exists(output_file)

    with open(output_file, mode='a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=["id", "title", "release_date", "popularity", "overview"])
        if not headers_written:
            writer.writeheader()
        writer.writerows(clean_records)
        print(f"Saved {len(clean_records)} new records to {output_file}.")

def save_raw_json(response_data, source_name):
    # Create a separate JSON file for each source
    filename = f"{source_name}.json"
    with open(filename, "a", encoding="utf-8") as f:
        json.dump(response_data, f, ensure_ascii=False, indent=4)
        f.write("\n")  # Ensure new line to prevent JSONDecodeError

# ---- Connection Test Function ----
def test_connection(source, config):
    success, error = source.check_connection(config)
    if success:
        print("✅ Connection successful!")
        return True
    else:
        print(f"❌ Connection failed: {error}")
        return False

# ---- Main Execution ----
if __name__ == "__main__":
    source = SourceTmdbApi()
    TmdbStream.last_fetched_date = load_last_fetched_date()  # Load the saved date

    # Run connection test
    if test_connection(source, config):
        for stream in source.streams(config):
            print(f"\nFetching data from {stream.name}...")
            records = list(stream.read_records(sync_mode=SyncMode.full_refresh))
            
            for record in records:
                save_raw_json(record.data, stream.name)
            
            # Save the latest fetched date
            if TmdbStream.last_fetched_date:
                save_last_fetched_date(TmdbStream.last_fetched_date)
                
            print(f"Fetched {len(records)} records from {stream.name}.")
