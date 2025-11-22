import os
from dotenv import load_dotenv

load_dotenv()

def get_str_env(name: str, default: str = "") -> str:
    value = os.getenv(name, "").strip()
    return value if value else default

def get_int_env(name: str, default: int = 0) -> int:
    value = os.getenv(name, "").strip()
    try:
        return int(value)
    except (ValueError, TypeError):
        return default

TMDB_API_KEY = get_str_env("TMDB_API_KEY", "")
PG_USER = get_str_env("PG_USER", "postgres")
PG_PASSWORD = get_str_env("PG_PASSWORD", "")
PG_DB = get_str_env("PG_DB", "tmdb_db")
PG_PORT = get_int_env("PG_PORT", 5432)
MAX_PAGES = get_int_env("MAX_PAGES", 5)
SCHEDULE = get_str_env("SCHEDULE", "0")
