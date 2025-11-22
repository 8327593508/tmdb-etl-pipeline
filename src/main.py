import os, sys, time
from config.config import MAX_PAGES, SCHEDULE, TMDB_API_KEY
from src.extract.tmdb_master_extract import extract_all_categories
from src.transform.transform_movies import transform_movies
from src.transform.transform_movie_details import transform_movie_details
from src.transform.transform_movie_credits import transform_movie_credits
from src.load.load_to_postgres import upsert_movies, upsert_movie_details, upsert_movie_credits
from src.utils.csv_export import export_to_csv
from src.utils.logger import get_logger

logger = get_logger("main")

def run_once():
    logger.info("="*70)
    logger.info("TMDB ETL PIPELINE STARTING")
    logger.info("="*70)

    if not TMDB_API_KEY:
        logger.error("TMDB_API_KEY not set!")
        sys.exit(1)

    try:
        logger.info("\n[1/6] EXTRACTING from TMDB...")
        data = extract_all_categories(pages_popular=MAX_PAGES, pages_top=MAX_PAGES)

        logger.info("\n[2/6] TRANSFORMING popular...")
        df_movies = transform_movies(data["popular"])

        logger.info("\n[3/6] TRANSFORMING details...")
        df_details = transform_movie_details(data["details"])

        logger.info("\n[4/6] TRANSFORMING credits...")
        df_credits = transform_movie_credits(data["credits"])

        logger.info("\n[5/6] LOADING to PostgreSQL...")
        upsert_movies(df_movies)
        upsert_movie_details(df_details)
        upsert_movie_credits(df_credits)

        logger.info("\n[6/6] EXPORTING to CSV...")
        export_to_csv()

        logger.info("\n"+"="*70)
        logger.info("ETL COMPLETED SUCCESSFULLY!")
        logger.info("="*70)
    except Exception as e:
        logger.error(f"ETL FAILED: {e}", exc_info=True)
        sys.exit(1)

def run_loop():
    interval = int(SCHEDULE) if SCHEDULE else 3600
    logger.info(f"Running ETL every {interval} seconds")
    while True:
        try:
            run_once()
        except Exception as e:
            logger.error(f"Pipeline error: {e}")
        time.sleep(interval)

if __name__ == "__main__":
    if os.environ.get("GITHUB_ACTIONS") == "true":
        run_once()
    else:
        if SCHEDULE and SCHEDULE.isdigit() and int(SCHEDULE) > 0:
            run_loop()
        else:
            run_once()
