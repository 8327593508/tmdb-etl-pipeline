import requests
from config.config import TMDB_API_KEY
from src.utils.session_retry import create_retry_session
from src.utils.logger import get_logger

logger = get_logger("tmdb_extract_credits")

def fetch_movie_credits(movie_id: int):
    try:
        session = create_retry_session()
        response = session.get(f"https://api.themoviedb.org/3/movie/{movie_id}/credits",
            params={"api_key": TMDB_API_KEY}, timeout=10)
        if response.status_code != 200:
            return None
        data = response.json()
        return {"movie_id": movie_id, "movie_cast": data.get("cast", []), "movie_crew": data.get("crew", [])}
    except Exception as e:
        logger.warning(f"Credits failed for {movie_id}: {e}")
        return None
