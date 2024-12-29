from source import SourceTmdbApi
from dotenv import load_dotenv
import os

load_dotenv()

api_key = os.getenv("TMDB_API_KEY")

if not api_key:
    raise ValueError("TMDB API key not found. Please check your .env file.")

config = {
    "api_key": api_key
}

source = SourceTmdbApi()
success, error = source.check_connection(logger=None, config=config)

if success:
    print("✅ Connection successful! API is reachable.")
else:
    print(f"❌ Connection failed: {error}")
