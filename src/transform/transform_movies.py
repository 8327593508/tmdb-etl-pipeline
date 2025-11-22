import pandas as pd
from src.utils.logger import get_logger

logger = get_logger("transform_movies")

def transform_movies(rows):
    logger.info(f"Transforming {len(rows)} movies")
    df = pd.DataFrame(rows)
    cols = ["id", "title", "vote_average", "vote_count", "popularity", "release_date", "original_language"]
    df = df[[c for c in cols if c in df.columns]]
    df["release_date"] = pd.to_datetime(df["release_date"], errors="coerce")
    return df.fillna(0)
