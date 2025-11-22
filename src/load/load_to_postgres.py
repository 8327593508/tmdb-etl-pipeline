from sqlalchemy import text
import pandas as pd
from src.utils.db_engine import get_engine
from src.utils.logger import get_logger

logger = get_logger("load_postgres")

def upsert_movies(df_movies):
    if df_movies.empty: logger.warning("No movies"); return
    logger.info(f"Loading {len(df_movies)} movies")
    engine = get_engine()
    with engine.begin() as conn:
        for _, row in df_movies.iterrows():
            conn.execute(text('''INSERT INTO popular_movies
                (id, title, vote_average, vote_count, popularity, release_date, original_language, last_updated)
                VALUES (:id, :title, :vote_average, :vote_count, :popularity, :release_date, :original_language, NOW())
                ON CONFLICT (id) DO UPDATE SET title = EXCLUDED.title, vote_average = EXCLUDED.vote_average,
                vote_count = EXCLUDED.vote_count, popularity = EXCLUDED.popularity, last_updated = NOW();'''), row.to_dict())

def upsert_movie_details(df_details):
    if df_details.empty: logger.warning("No details"); return
    logger.info(f"Loading {len(df_details)} details")
    engine = get_engine()
    with engine.begin() as conn:
        for _, row in df_details.iterrows():
            data = row.to_dict()
            for key in data:
                if pd.isna(data[key]): data[key] = None
            conn.execute(text('''INSERT INTO movie_details
                (id, title, overview, release_date, popularity, vote_count, vote_average, poster_path, backdrop_path,
                 original_language, genres, runtime, budget, revenue, homepage, tagline, status, imdb_id,
                 production_companies, production_countries, spoken_languages, last_updated)
                VALUES (:id, :title, :overview, :release_date, :popularity, :vote_count, :vote_average, :poster_path,
                        :backdrop_path, :original_language, :genres::jsonb, :runtime, :budget, :revenue, :homepage,
                        :tagline, :status, :imdb_id, :production_companies::jsonb, :production_countries::jsonb,
                        :spoken_languages::jsonb, NOW())
                ON CONFLICT (id) DO UPDATE SET overview = EXCLUDED.overview, last_updated = NOW();'''), data)

def upsert_movie_credits(df_credits):
    if df_credits.empty: logger.warning("No credits"); return
    logger.info(f"Loading {len(df_credits)} credits")
    engine = get_engine()
    with engine.begin() as conn:
        for _, row in df_credits.iterrows():
            conn.execute(text('''INSERT INTO movie_credits (movie_id, movie_cast, movie_crew, last_updated)
                VALUES (:movie_id, :movie_cast::jsonb, :movie_crew::jsonb, NOW())
                ON CONFLICT (movie_id) DO UPDATE SET movie_cast = EXCLUDED.movie_cast,
                movie_crew = EXCLUDED.movie_crew, last_updated = NOW();'''), row.to_dict())
