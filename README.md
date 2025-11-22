# TMDB ETL Pipeline

Complete ETL pipeline for The Movie Database (TMDB).

## Quick Start

```bash
cp .env.example .env
# Edit .env with TMDB_API_KEY

docker-compose up --build
```

## Output
- data/popular_movies.csv
- data/movie_details.csv
- data/movie_credits.csv

## Local Setup
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.main
```
