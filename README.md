# 🎬 TMDB ETL Pipeline

[![Daily TMDB ETL](https://github.com/yourusername/tmdb-etl-pipeline/actions/workflows/pipeline.yml/badge.svg)](https://github.com/yourusername/tmdb-etl-pipeline/actions/workflows/pipeline.yml)

A production-ready ETL (Extract, Transform, Load) pipeline that automatically fetches movie data from The Movie Database (TMDB) API, processes it, stores it in PostgreSQL, and exports it as CSV files for Power BI dashboard visualization.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Project Structure](#project-structure)
- [Data Schema](#data-schema)
- [Usage](#usage)
- [Power BI Integration](#power-bi-integration)
- [GitHub Actions Automation](#github-actions-automation)
- [Troubleshooting](#troubleshooting)

## 🎯 Overview

This pipeline automatically collects comprehensive movie data from TMDB, including:
- **Popular Movies**: Top trending films
- **Movie Details**: Complete metadata (budget, revenue, runtime, genres, etc.)
- **Movie Credits**: Full cast and crew information

The data is processed, stored in PostgreSQL, and exported as CSV files that can be connected to Power BI for real-time dashboard updates.

## ✨ Features

- **Automated Daily Updates**: GitHub Actions workflow runs daily at 4 AM UTC
- **Multi-Category Extraction**: Popular, top-rated, upcoming, and trending movies
- **Robust Error Handling**: Retry logic for API calls and comprehensive logging
- **Database Storage**: PostgreSQL with optimized schema and indexes
- **CSV Export**: Automated export for Power BI integration
- **Docker Support**: Fully containerized for consistent deployment
- **Configurable**: Easy configuration via environment variables

## 🏗️ Architecture

```
┌─────────────────┐
│   TMDB API      │
└────────┬────────┘
         │ Extract
         ▼
┌─────────────────┐
│   ETL Pipeline  │
│  (Python + Docker)│
└────────┬────────┘
         │ Transform & Load
         ▼
┌─────────────────┐
│   PostgreSQL    │
└────────┬────────┘
         │ Export
         ▼
┌─────────────────┐
│   CSV Files     │
└────────┬────────┘
         │ Connect
         ▼
┌─────────────────┐
│   Power BI      │
│   Dashboard     │
└─────────────────┘
```

## 📦 Prerequisites

- **TMDB API Key**: [Get one here](https://www.themoviedb.org/settings/api)
- **Docker & Docker Compose**: [Install Docker](https://docs.docker.com/get-docker/)
- **GitHub Account**: For automated pipeline execution
- **Power BI Desktop**: For dashboard creation

## 🚀 Quick Start

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/tmdb-etl-pipeline.git
cd tmdb-etl-pipeline
```

### 2. Configure Environment

```bash
cp .env.example .env
```

Edit `.env` and add your TMDB API key:

```env
TMDB_API_KEY=your_actual_api_key_here
PG_USER=tmdb_user
PG_PASSWORD=tmdb_password
PG_HOST=localhost
PG_DB=tmdb_db
PG_PORT=5432
MAX_PAGES=5
SCHEDULE=0
```

### 3. Run with Docker

```bash
docker-compose up --build
```

The pipeline will:
1. Start PostgreSQL database
2. Run the ETL process
3. Export CSV files to the `data/` folder

### 4. Access the Data

- **PostgreSQL**: Connect to `localhost:5432` with credentials from `.env`
- **CSV Files**: Available in `data/` directory
  - `popular_movies.csv`
  - `movie_details.csv`
  - `movie_credits.csv`

## ⚙️ Configuration

### Environment Variables

| Variable | Description | Default |
|----------|-------------|---------|
| `TMDB_API_KEY` | Your TMDB API key | Required |
| `PG_USER` | PostgreSQL username | `tmdb_user` |
| `PG_PASSWORD` | PostgreSQL password | `tmdb_password` |
| `PG_HOST` | Database host | `localhost` |
| `PG_DB` | Database name | `tmdb_db` |
| `PG_PORT` | Database port | `5432` |
| `MAX_PAGES` | Pages to fetch per category | `5` |
| `SCHEDULE` | Run interval in seconds (0=once) | `0` |

### Adjusting Data Volume

To fetch more or less data, modify `MAX_PAGES`:
- `MAX_PAGES=5`: ~100 movies (recommended for testing)
- `MAX_PAGES=10`: ~200 movies (recommended for production)
- `MAX_PAGES=20`: ~400 movies (comprehensive dataset)

## 📁 Project Structure

```
tmdb-etl-pipeline/
├── .github/
│   └── workflows/
│       └── pipeline.yml          # GitHub Actions automation
├── config/
│   └── config.py                 # Configuration management
├── docker/
│   └── Dockerfile                # Docker image definition
├── sql/
│   └── create_tables.sql         # Database schema
├── src/
│   ├── extract/                  # Data extraction modules
│   │   ├── tmdb_extract_movies.py
│   │   ├── tmdb_extract_details.py
│   │   ├── tmdb_extract_credits.py
│   │   └── tmdb_master_extract.py
│   ├── transform/                # Data transformation modules
│   │   ├── transform_movies.py
│   │   ├── transform_movie_details.py
│   │   └── transform_movie_credits.py
│   ├── load/                     # Data loading modules
│   │   └── load_to_postgres.py
│   ├── utils/                    # Utility functions
│   │   ├── logger.py
│   │   ├── db_engine.py
│   │   ├── session_retry.py
│   │   └── csv_export.py
│   └── main.py                   # Main ETL orchestrator
├── data/                         # CSV export directory
├── docker-compose.yml            # Local development
├── docker-compose.ci.yml         # CI/CD configuration
├── requirements.txt              # Python dependencies
└── README.md                     # This file
```

## 🗄️ Data Schema

### popular_movies
- `id` (Primary Key)
- `title`
- `vote_average`
- `vote_count`
- `popularity`
- `release_date`
- `original_language`
- `last_updated`

### movie_details
- `id` (Primary Key)
- `title`, `overview`, `tagline`
- `release_date`, `runtime`, `status`
- `budget`, `revenue`
- `popularity`, `vote_average`, `vote_count`
- `genres`, `production_companies`, `production_countries` (JSONB)
- `poster_path`, `backdrop_path`
- `homepage`, `imdb_id`
- `last_updated`

### movie_credits
- `movie_id` (Primary Key)
- `movie_cast` (JSONB array)
- `movie_crew` (JSONB array)
- `last_updated`

## 💻 Usage

### Local Development

```bash
# Install dependencies
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install -r requirements.txt

# Run pipeline
python -m src.main
```

### Scheduled Execution

Set `SCHEDULE` in `.env` to run repeatedly:

```env
SCHEDULE=3600  # Run every hour
```

### Manual Docker Run

```bash
# Build image
docker-compose build

# Run once
docker-compose run --rm app

# Run in background
docker-compose up -d
```

## 📊 Power BI Integration

### Step 1: Connect to CSV Files

1. Open Power BI Desktop
2. Go to **Home** → **Get Data** → **Text/CSV**
3. Navigate to your `data/` folder and select the CSV files
4. Load all three tables: `popular_movies`, `movie_details`, `movie_credits`

### Step 2: Set Up Relationships

In Power BI Model view, create relationships:
- `popular_movies[id]` → `movie_details[id]`
- `movie_details[id]` → `movie_credits[movie_id]`

### Step 3: Configure Auto-Refresh

For real-time updates:

**Option A: Folder Connection (Recommended)**
1. Get Data → **Folder**
2. Point to your `data/` folder
3. Power BI will automatically refresh when files are updated

**Option B: GitHub Direct Connection**
1. Get Data → **Web**
2. Use raw GitHub URLs:
   ```
   https://raw.githubusercontent.com/yourusername/tmdb-etl-pipeline/main/data/popular_movies.csv
   ```
3. Set refresh schedule in Power BI Service

### Step 4: Create Dashboard

Example visualizations:
- **KPI Cards**: Total movies, average rating, total revenue
- **Bar Chart**: Top 10 movies by popularity
- **Line Chart**: Movies released over time
- **Pie Chart**: Genre distribution
- **Table**: Top rated movies with details
- **Map**: Production countries

  ### Dashboard Overview
  
  <img width="1325" height="746" alt="image" src="https://github.com/user-attachments/assets/faf24a0e-6d0b-47d0-8af3-b9e9ec5a7a9d" />


## 🔄 GitHub Actions Automation

The pipeline runs automatically via GitHub Actions:

### Schedule
- **Daily**: 4:00 AM UTC
- **Manual**: Via workflow dispatch

### Setup GitHub Secrets

Go to **Settings** → **Secrets and variables** → **Actions** and add:

```
TMDB_API_KEY=your_api_key
PG_USER=tmdb_user
PG_PASSWORD=tmdb_password
PG_DB=tmdb_db
MAX_PAGES=10
```

### Workflow Steps

1. ✅ Checkout repository
2. ✅ Create environment configuration
3. ✅ Start PostgreSQL container
4. ✅ Run ETL pipeline
5. ✅ Export data to CSV files
6. ✅ Commit CSV files to repository
7. ✅ Cleanup containers

### Monitoring

Check pipeline status at:
```
https://github.com/yourusername/tmdb-etl-pipeline/actions
```

## 🐛 Troubleshooting

### Issue: API Rate Limiting

**Solution**: Adjust `time.sleep()` values in `tmdb_master_extract.py` or reduce `MAX_PAGES`

### Issue: Database Connection Failed

**Solution**: 
```bash
# Check if PostgreSQL is running
docker ps

# Check logs
docker logs tmdb_postgres

# Restart containers
docker-compose restart
```

### Issue: CSV Files Not Generated

**Solution**:
```bash
# Ensure data directory exists
mkdir -p data

# Check permissions
chmod 755 data/

# Run export manually
docker-compose run --rm app python -m src.utils.csv_export
```

### Issue: GitHub Actions Failing

**Solution**:
- Verify all secrets are set correctly
- Check workflow logs for specific errors
- Ensure repository has write permissions enabled

## 📝 License

This project is licensed under the MIT License.

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📧 Contact

For questions or support, please open an issue in the repository.

---

