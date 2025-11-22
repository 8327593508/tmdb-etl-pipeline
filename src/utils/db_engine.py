from sqlalchemy import create_engine
from config.config import PG_USER, PG_PASSWORD, PG_DB, PG_PORT
import os

PG_HOST = os.getenv("PG_HOST", "localhost")
DB_URL = f"postgresql://{PG_USER}:{PG_PASSWORD}@{PG_HOST}:{PG_PORT}/{PG_DB}"

def get_engine():
    try:
        return create_engine(DB_URL, pool_pre_ping=True)
    except Exception as e:
        print(f"Database error: {e}")
        raise
