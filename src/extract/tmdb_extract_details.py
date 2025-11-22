import requests
from config.config import TMDB_API_KEY
from src.utils.logger import get_logger

logger = get_logger("tmdb_extract_details")

def fetch_movie_details(movie_id: int):
    try:
        response = requests.get(f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_API_KEY}", timeout=10)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logger.warning(f"Details failed for {movie_id}: {e}")
        return None
