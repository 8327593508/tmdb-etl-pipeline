import pandas as pd
import json
from src.utils.logger import get_logger

logger = get_logger("transform_movie_credits")

def transform_movie_credits(rows):
    logger.info(f"Transforming {len(rows)} credits")
    df = pd.DataFrame(rows)
    if "movie_cast" in df.columns:
        df["movie_cast"] = df["movie_cast"].apply(lambda x: json.dumps(x) if isinstance(x, list) else json.dumps([]))
    if "movie_crew" in df.columns:
        df["movie_crew"] = df["movie_crew"].apply(lambda x: json.dumps(x) if isinstance(x, list) else json.dumps([]))
    return df
