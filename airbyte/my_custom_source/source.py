import requests
from airbyte_cdk.sources import AbstractSource
from airbyte_cdk.models import AirbyteStream, SyncMode, AirbyteRecordMessage
from airbyte_cdk.sources.streams.http import HttpStream
from datetime import datetime
from typing import Iterable, Any, Mapping, Optional

class SourceTmdbApi(AbstractSource):
    def check_connection(self, logger, config) -> tuple[bool, any]:
        url = "https://api.themoviedb.org/3/movie/popular"
        headers = {"Authorization": f"Bearer {config['api_key']}"}
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return True, None
        except requests.exceptions.RequestException as e:
            return False, f"Connection failed: {str(e)}"

    def streams(self, config: Mapping[str, Any]) -> list:
        return [
            TmdbStream(config=config, endpoint="movie/popular", name="popular_movies"),
            TmdbStream(config=config, endpoint="movie/top_rated", name="top_rated_movies"),
            TmdbStream(config=config, endpoint="movie/upcoming", name="upcoming_movies"),
        ]


class TmdbStream(HttpStream):
    url_base = "https://api.themoviedb.org/3/"
    primary_key = "id"

    def __init__(self, config: Mapping[str, Any], endpoint: str, name: str):
        super().__init__()
        self.api_key = config["api_key"]
        self.endpoint = endpoint
        self.name = name

    def path(self, **kwargs) -> str:
        return self.endpoint

    def next_page_token(self, response: requests.Response) -> Optional[Mapping[str, Any]]:
        data = response.json()
        if data["page"] < data["total_pages"]:
            return {"page": data["page"] + 1}
        return None

    def request_params(self, stream_state: Mapping[str, Any], next_page_token: Mapping[str, Any] = None) -> Mapping[str, Any]:
        params = {"page": next_page_token["page"] if next_page_token else 1}
        return params

    def request_headers(self, **kwargs) -> Mapping[str, str]:
        return {"Authorization": f"Bearer {self.api_key}"}

    def parse_response(self, response: requests.Response, **kwargs) -> Iterable[Mapping]:
        data = response.json()
        for movie in data.get("results", []):
            yield {
                "id": movie.get("id"),
                "title": movie.get("title"),
                "release_date": movie.get("release_date"),
                "popularity": movie.get("popularity"),
                "overview": movie.get("overview"),
            }

    def read_records(self, sync_mode: SyncMode, stream_slice: Mapping[str, Any] = None, stream_state: Mapping[str, Any] = None) -> Iterable[Mapping[str, Any]]:
        for record in super().read_records(sync_mode, stream_slice, stream_state):
            yield AirbyteRecordMessage(
                stream=self.name,
                data=record,
                emitted_at=int(datetime.now().timestamp()) * 1000
            )
