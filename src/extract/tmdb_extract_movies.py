import requests
from config.config import TMDB_API_KEY
from src.utils.session_retry import create_retry_session

BASE_URL = "https://api.themoviedb.org/3/movie"

def fetch_popular_movies(page: int):
    session = create_retry_session()
    response = session.get(f"{BASE_URL}/popular",
        params={"api_key": TMDB_API_KEY, "page": page, "language": "en-US"}, timeout=20)
    response.raise_for_status()
    return response.json().get("results", [])

def fetch_top_rated_movies(page: int):
    session = create_retry_session()
    response = session.get(f"{BASE_URL}/top_rated",
        params={"api_key": TMDB_API_KEY, "page": page, "language": "en-US"}, timeout=20)
    response.raise_for_status()
    return response.json().get("results", [])

def fetch_trending_movies(page: int):
    session = create_retry_session()
    response = session.get("https://api.themoviedb.org/3/trending/movie/day",
        params={"api_key": TMDB_API_KEY, "page": page}, timeout=20)
    response.raise_for_status()
    return response.json().get("results", [])

def fetch_upcoming_movies(page: int):
    session = create_retry_session()
    response = session.get(f"{BASE_URL}/upcoming",
        params={"api_key": TMDB_API_KEY, "page": page, "language": "en-US"}, timeout=20)
    response.raise_for_status()
    return response.json().get("results", [])
