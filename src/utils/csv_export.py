import pandas as pd
from src.utils.db_engine import get_engine
from src.utils.logger import get_logger
from pathlib import Path

logger = get_logger("csv_export")

def export_to_csv():
    try:
        Path("data").mkdir(exist_ok=True)
        engine = get_engine()

        logger.info("Exporting popular_movies...")
        df = pd.read_sql_table("popular_movies", engine)
        df.to_csv("data/popular_movies.csv", index=False)

        logger.info("Exporting movie_details...")
        df = pd.read_sql_table("movie_details", engine)
        df.to_csv("data/movie_details.csv", index=False)

        logger.info("Exporting movie_credits...")
        df = pd.read_sql_table("movie_credits", engine)
        df.to_csv("data/movie_credits.csv", index=False)

        logger.info("✓ CSV export complete")
    except Exception as e:
        logger.error(f"Export failed: {e}")
