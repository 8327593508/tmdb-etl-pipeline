import json
from sqlalchemy import text
from src.utils.db_engine import get_engine
from src.utils.logger import get_logger

logger = get_logger("load_movie_credits")


def upsert_movie_credits(df_credits):
    """Upsert movie credits into movie_credits table."""
    
    if df_credits.empty:
        logger.warning("No credits to insert")
        return
    
    engine = get_engine()
    
    try:
        rows = df_credits.to_dict(orient="records")
        
        with engine.begin() as conn:
            for row in rows:
                movie_id = int(row.get("movie_id", 0))
                cast = json.dumps(row.get("movie_cast", []))
                crew = json.dumps(row.get("movie_crew", []))
                
                sql = text("""
                    INSERT INTO movie_credits (movie_id, movie_cast, movie_crew)
                    VALUES (:movie_id, :cast, :crew)
                    ON CONFLICT (movie_id)
                    DO UPDATE SET
                        movie_cast = EXCLUDED.movie_cast,
                        movie_crew = EXCLUDED.movie_crew,
                        last_updated = CURRENT_TIMESTAMP;
                """)
                
                conn.execute(sql, {
                    "movie_id": movie_id,
                    "cast": cast,
                    "crew": crew
                })
        
        logger.info(f"Successfully inserted {len(df_credits)} movie credits")
        
    except Exception as e:
        logger.error(f"Error inserting movie credits: {e}", exc_info=True)
        raise
