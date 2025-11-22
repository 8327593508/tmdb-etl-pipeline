from src.extract.tmdb_extract_movies import *
from src.extract.tmdb_extract_details import fetch_movie_details
from src.extract.tmdb_extract_credits import fetch_movie_credits
from src.utils.logger import get_logger
from config.config import MAX_PAGES
import time

logger = get_logger("tmdb_master_extract")

def extract_all_categories(pages_popular=None, pages_top=None, pages_upcoming=None, pages_trending=None):
    pages_popular = pages_popular or MAX_PAGES
    pages_top = pages_top or MAX_PAGES
    pages_upcoming = pages_upcoming or 2
    pages_trending = pages_trending or 2

    logger.info("Fetching from TMDB...")
    popular, top_rated, upcoming, trending = [], [], [], []

    for page in range(1, pages_popular + 1):
        try: popular.extend(fetch_popular_movies(page))
        except Exception as e: logger.error(f"Popular page {page}: {e}")
        time.sleep(0.25)

    for page in range(1, pages_top + 1):
        try: top_rated.extend(fetch_top_rated_movies(page))
        except Exception as e: logger.error(f"Top page {page}: {e}")
        time.sleep(0.25)

    for page in range(1, pages_upcoming + 1):
        try: upcoming.extend(fetch_upcoming_movies(page))
        except Exception as e: logger.error(f"Upcoming page {page}: {e}")
        time.sleep(0.25)

    for page in range(1, pages_trending + 1):
        try: trending.extend(fetch_trending_movies(page))
        except Exception as e: logger.error(f"Trending page {page}: {e}")
        time.sleep(0.25)

    movie_ids = set()
    for movies in [popular, top_rated, upcoming, trending]:
        for m in movies:
            if m and m.get("id"): movie_ids.add(m["id"])

    logger.info(f"Processing {len(movie_ids)} unique movies...")
    details_list, credits_list = [], []

    for idx, movie_id in enumerate(movie_ids, 1):
        if idx % 50 == 0: logger.info(f"Progress: {idx}/{len(movie_ids)}")
        d = fetch_movie_details(movie_id)
        if d: details_list.append(d)
        c = fetch_movie_credits(movie_id)
        if c: credits_list.append(c)
        time.sleep(0.25)

    logger.info(f"Extracted {len(details_list)} details, {len(credits_list)} credits")
    return {"popular": popular, "top_rated": top_rated, "upcoming": upcoming, "trending": trending, "details": details_list, "credits": credits_list}
