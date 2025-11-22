# src/main.py

import os
import sys
from config.config import MAX_PAGES, SCHEDULE, TMDB_API_KEY
from src.extract.tmdb_master_extract import extract_all_categories
from src.transform.transform_movies import transform_movies
from src.transform.transform_movie_details import transform_movie_details
from src.transform.transform_movie_credits import transform_movie_credits
from src.load.load_to_postgres import upsert_movies, upsert_movie_details, upsert_movie_credits
from src.utils.logger import get_logger
import time

logger = get_logger("main")


def run_once():
    """Execute ETL pipeline once."""
    logger.info("=== Starting TMDB multi-file ETL run ===")
    
    # Validate API key
    if not TMDB_API_KEY or TMDB_API_KEY == "":
        logger.error("TMDB_API_KEY is not set in environment!")
        sys.exit(1)
    
    logger.info(f"Using MAX_PAGES: {MAX_PAGES}")
    
    try:
        logger.info("Step 1: Extracting data from TMDB API...")
        data = extract_all_categories(
            pages_popular=MAX_PAGES,
            pages_top=MAX_PAGES,
            pages_upcoming=2,
            pages_trending=2
        )
        
        popular_movies = data.get("popular", [])
        details_list = data.get("details", [])
        credits_list = data.get("credits", [])
        
        logger.info(f"Extracted: {len(popular_movies)} popular, {len(details_list)} details, {len(credits_list)} credits")
        
        # Transform
        logger.info("Step 2: Transforming popular movies...")
        df_movies = transform_movies(popular_movies)
        logger.info(f"Transformed {len(df_movies)} popular movies")
        
        logger.info("Step 3: Transforming movie details...")
        df_details = transform_movie_details(details_list)
        logger.info(f"Transformed {len(df_details)} movie details")
        
        logger.info("Step 4: Transforming movie credits...")
        df_credits = transform_movie_credits(credits_list)
        logger.info(f"Transformed {len(df_credits)} movie credits")
        
        # Load
        logger.info("Step 5: Loading into Postgres...")
        
        if not df_movies.empty:
            logger.info(f"Loading {len(df_movies)} popular movies...")
            upsert_movies(df_movies)
        else:
            logger.warning("No popular movies to load")
        
        if not df_details.empty:
            logger.info(f"Loading {len(df_details)} movie details...")
            upsert_movie_details(df_details)
        else:
            logger.warning("No movie details to load")
        
        if not df_credits.empty:
            logger.info(f"Loading {len(df_credits)} movie credits...")
            upsert_movie_credits(df_credits)
        else:
            logger.warning("No movie credits to load")
        
        logger.info("=== ETL COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logger.error(f"ETL failed with error: {e}", exc_info=True)
        sys.exit(1)


def run_loop():
    """Local-only: repeatedly run ETL based on SCHEDULE seconds."""
    try:
        interval = int(SCHEDULE) if SCHEDULE else 3600
    except Exception:
        logger.error("SCHEDULE must be an integer number of seconds.")
        interval = 3600
    
    logger.info(f"Starting ETL loop with {interval} second interval...")
    
    while True:
        try:
            run_once()
        except Exception as e:
            logger.error(f"Error in loop iteration: {e}")
        
        logger.info(f"Sleeping for {interval} seconds before next run...")
        time.sleep(interval)


if __name__ == "__main__":
    # In GitHub Actions we set GITHUB_ACTIONS=true so it runs once
    if os.environ.get("GITHUB_ACTIONS") == "true":
        logger.info("Running in GitHub Actions mode (single run)")
        run_once()
    else:
        # Local mode: check if we should loop or run once
        schedule_val = os.environ.get("SCHEDULE", "0")
        if schedule_val and str(schedule_val).isdigit() and int(schedule_val) > 0:
            logger.info("Running in loop mode")
            run_loop()
        else:
            logger.info("Running in single-run mode")
            run_once()
