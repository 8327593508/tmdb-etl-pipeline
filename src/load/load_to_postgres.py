import json
import pandas as pd
from sqlalchemy import text
from src.utils.db_engine import get_engine
from src.utils.logger import get_logger

logger = get_logger("load_postgres")


def clean_row(row_dict):
    """Convert NaN → None and convert JSON fields to JSON strings."""
    clean = {}

    for key, value in row_dict.items():
        # Convert NaN -> None
        if pd.isna(value):
            clean[key] = None
            continue

        clean[key] = value

    return clean


def json_or_none(value):
    """Convert dict/list to JSON string or return None."""
    if value is None:
        return None
    return json.dumps(value)


# ---------------------------------------------------------------------
# 1️⃣ UPSERT POPULAR MOVIES
# ---------------------------------------------------------------------

def upsert_movies(df_movies):
    if df_movies.empty:
        logger.warning("No movies")
        return

    logger.info(f"Loading {len(df_movies)} movies")
    engine = get_engine()

    with engine.begin() as conn:
        for _, row in df_movies.iterrows():
            data = clean_row(row.to_dict())

            conn.execute(
                text('''
                    INSERT INTO popular_movies
                    (id, title, vote_average, vote_count, popularity, release_date, original_language, last_updated)
                    VALUES (:id, :title, :vote_average, :vote_count, :popularity, :release_date, :original_language, NOW())
                    ON CONFLICT (id) DO UPDATE
                    SET title = EXCLUDED.title,
                        vote_average = EXCLUDED.vote_average,
                        vote_count = EXCLUDED.vote_count,
                        popularity = EXCLUDED.popularity,
                        last_updated = NOW();
                '''),

                data
            )


# ---------------------------------------------------------------------
# 2️⃣ UPSERT MOVIE DETAILS
# ---------------------------------------------------------------------

def upsert_movie_details(df_details):
    if df_details.empty:
        logger.warning("No details")
        return

    logger.info(f"Loading {len(df_details)} details")
    engine = get_engine()

    with engine.begin() as conn:
        for _, row in df_details.iterrows():
            data = clean_row(row.to_dict())

            # Convert JSON-like columns
            data["genres"] = json_or_none(data.get("genres"))
            data["production_companies"] = json_or_none(data.get("production_companies"))
            data["production_countries"] = json_or_none(data.get("production_countries"))
            data["spoken_languages"] = json_or_none(data.get("spoken_languages"))

            conn.execute(
                text('''
                    INSERT INTO movie_details
                    (id, title, overview, release_date, popularity, vote_count, vote_average, poster_path, backdrop_path,
                     original_language, genres, runtime, budget, revenue, homepage, tagline, status, imdb_id,
                     production_companies, production_countries, spoken_languages, last_updated)

                    VALUES (:id, :title, :overview, :release_date, :popularity, :vote_count, :vote_average,
                            :poster_path, :backdrop_path, :original_language,
                            CAST(:genres AS jsonb), :runtime, :budget, :revenue, :homepage, :tagline, :status, :imdb_id,
                            CAST(:production_companies AS jsonb),
                            CAST(:production_countries AS jsonb),
                            CAST(:spoken_languages AS jsonb),
                            NOW())

                    ON CONFLICT (id) DO UPDATE
                    SET overview = EXCLUDED.overview,
                        last_updated = NOW();
                '''),

                data
            )


# ---------------------------------------------------------------------
# 3️⃣ UPSERT MOVIE CREDITS
# ---------------------------------------------------------------------

def upsert_movie_credits(df_credits):
    if df_credits.empty:
        logger.warning("No credits")
        return

    logger.info(f"Loading {len(df_credits)} credits")
    engine = get_engine()

    with engine.begin() as conn:
        for _, row in df_credits.iterrows():
            data = clean_row(row.to_dict())

            # Convert cast/crew to json
            data["movie_cast"] = json_or_none(data.get("movie_cast"))
            data["movie_crew"] = json_or_none(data.get("movie_crew"))

            conn.execute(
                text('''
                    INSERT INTO movie_credits
                    (movie_id, movie_cast, movie_crew, last_updated)
                    VALUES (:movie_id,
                            CAST(:movie_cast AS jsonb),
                            CAST(:movie_crew AS jsonb),
                            NOW())

                    ON CONFLICT (movie_id) DO UPDATE
                    SET movie_cast = EXCLUDED.movie_cast,
                        movie_crew = EXCLUDED.movie_crew,
                        last_updated = NOW();
                '''),

                data
            )
