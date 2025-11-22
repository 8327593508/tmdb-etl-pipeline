import pandas as pd
import json
from src.utils.logger import get_logger

logger = get_logger("transform_movie_details")

def transform_movie_details(rows):
    logger.info(f"Transforming {len(rows)} details")
    df = pd.DataFrame(rows)
    for col in ["genres", "production_companies", "production_countries", "spoken_languages"]:
        if col in df.columns:
            df[col] = df[col].apply(lambda x: json.dumps(x) if isinstance(x, list) else json.dumps([]))
    return df
