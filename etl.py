"""
ETL pipeline: extract MovieLens CSVs, enrich with OMDb API, transform and clean, load into SQLite.
"""

import os
import re
import time
import json
import sqlite3
import logging
import argparse

import pandas as pd
import requests
from dotenv import load_dotenv

# Load environment variables from .env if present (without overriding existing environment variables)
load_dotenv(override=False)

# Configure readable logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("movie_pipeline")

# File paths
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
MOVIES_CSV = os.path.join(DATA_DIR, "movies.csv")
RATINGS_CSV = os.path.join(DATA_DIR, "ratings.csv")
LINKS_CSV = os.path.join(DATA_DIR, "links.csv")
SCHEMA_SQL = os.path.join(BASE_DIR, "schema.sql")
QUERIES_SQL = os.path.join(BASE_DIR, "queries.sql")
DB_FILE = os.path.join(BASE_DIR, "movies.db")
CACHE_FILE = os.path.join(BASE_DIR, "omdb_cache.json")


# -----------------------------------------------------------------------------
# Helper Functions for Transformation and Cleaning
# -----------------------------------------------------------------------------
def extract_year_and_clean_title(title):
    """
    Extracts the 4-digit release year from the title string and returns clean title.
    Example: 'Toy Story (1995)' -> (1995, 'Toy Story')
    """
    if not isinstance(title, str) or not title.strip():
        return None, ""

    title_str = title.strip()
    match = re.search(r"\((\d{4})\)$", title_str)
    if match:
        year = int(match.group(1))
        clean_title = re.sub(r"\s*\(\d{4}\)$", "", title_str).strip()
        return year, clean_title if clean_title else title_str
    return None, title_str


def get_decade(year):
    """Calculates the decade string for a year (e.g. 1995 -> '1990s')."""
    if year is not None and pd.notnull(year):
        try:
            return f"{int(year) // 10 * 10}s"
        except (ValueError, TypeError):
            return None
    return None


def clean_box_office(val):
    """Converts box office string to integer USD (e.g. '$373,554,033' -> 373554033)."""
    if val is None or pd.isna(val):
        return None
    val_str = str(val).strip()
    if val_str in ["N/A", "", "None"]:
        return None
    cleaned = re.sub(r"[^\d]", "", val_str)
    return int(cleaned) if cleaned else None


def clean_runtime(val):
    """Extracts minutes as integer from runtime string (e.g. '81 min' -> 81)."""
    if val is None or pd.isna(val):
        return None
    val_str = str(val).strip()
    if val_str in ["N/A", "", "None"]:
        return None
    match = re.search(r"\d+", val_str)
    return int(match.group()) if match else None


def clean_string(val):
    """Cleans text fields, converting 'N/A' or blanks to None."""
    if val is None or pd.isna(val):
        return None
    val_str = str(val).strip()
    if val_str in ["N/A", "", "None"]:
        return None
    return val_str


def parse_directors(raw_director):
    """
    Splits comma-separated director names into a clean list of names.
    Example: 'Joel Coen, Ethan Coen' -> ['Joel Coen', 'Ethan Coen']
    """
    if raw_director is None or pd.isna(raw_director):
        return []
    val_str = str(raw_director).strip()
    if val_str in ["N/A", "", "None"]:
        return []
    return [name.strip() for name in val_str.split(",") if name.strip() and name.strip() != "N/A"]


# -----------------------------------------------------------------------------
# Caching Layer for OMDb API
# -----------------------------------------------------------------------------
def load_cache():
    """Loads existing OMDb API responses from local JSON cache."""
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
        except Exception as e:
            logger.warning("Could not read cache: %s. Starting with empty cache.", e)
    return {}


def save_cache(cache):
    """Saves API responses to local JSON cache to prevent duplicate calls."""
    try:
        with open(CACHE_FILE, "w", encoding="utf-8") as f:
            json.dump(cache, f, indent=2, ensure_ascii=False)
    except Exception as e:
        logger.warning("Failed to save cache: %s", e)


# -----------------------------------------------------------------------------
# Step 1: EXTRACT
# -----------------------------------------------------------------------------
def extract_csv_data(movies_path=MOVIES_CSV, ratings_path=RATINGS_CSV, links_path=LINKS_CSV):
    """
    Reads MovieLens CSV files into Pandas DataFrames.
    Merges IMDb IDs from links.csv to enable exact lookups in the OMDb API.
    """
    logger.info("[Step 1/4] EXTRACT: Reading CSV files...")

    if not os.path.exists(movies_path) or not os.path.exists(ratings_path):
        raise FileNotFoundError(f"Missing required CSVs: {movies_path} or {ratings_path}")

    movies_df = pd.read_csv(movies_path)
    ratings_df = pd.read_csv(ratings_path)
    logger.info("  - Loaded %d movies from %s", len(movies_df), os.path.basename(movies_path))
    logger.info("  - Loaded %d ratings from %s", len(ratings_df), os.path.basename(ratings_path))

    # We use links.csv to get the exact IMDb ID (e.g. tt0114709) for each movie.
    # Searching OMDb by exact ID is 100% accurate, whereas searching by title
    # can fail on punctuation or return the wrong remake.
    if os.path.exists(links_path):
        links_df = pd.read_csv(links_path)
        # Format into standard 7-digit IMDb format (e.g. 114709 -> tt0114709)
        links_df["imdb_id"] = links_df["imdbId"].apply(
            lambda x: f"tt{str(int(x)).zfill(7)}" if pd.notnull(x) else None
        )
        movies_df = movies_df.merge(links_df[["movieId", "imdb_id"]], on="movieId", how="left")
        logger.info("  - Merged exact IMDb IDs from links.csv")
    else:
        movies_df["imdb_id"] = None

    return movies_df, ratings_df


# -----------------------------------------------------------------------------
# Step 2: ENRICH (OMDb API Integration)
# -----------------------------------------------------------------------------
def fetch_movie_from_api(api_key, imdb_id=None, title=None, session=None, max_retries=3):
    """
    Queries OMDb API with simple retry on network errors or 5xx server errors.
    Returns: (data_dict, error_status)
    """
    url = "http://www.omdbapi.com/"
    params = {"apikey": api_key}
    if imdb_id:
        params["i"] = imdb_id
    elif title:
        _, clean_name = extract_year_and_clean_title(title)
        params["t"] = clean_name
    else:
        return None, "NO_QUERY_PARAM"

    http = session or requests.Session()

    for attempt in range(1, max_retries + 1):
        try:
            response = http.get(url, params=params, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if data.get("Response") == "True":
                    return data, None

                err = data.get("Error", "")
                if "Movie not found" in err:
                    return None, "NOT_FOUND"
                if "Request limit reached" in err:
                    return None, "LIMIT_REACHED"
                if "Invalid API key" in err:
                    return None, "INVALID_KEY"
                return None, "OTHER_ERROR"

            # If server error or rate limited, retry after a short pause
            if response.status_code in (429, 500, 502, 503, 504):
                sleep_sec = 2 ** attempt
                time.sleep(sleep_sec)
                continue

            return None, "HTTP_ERROR"

        except (requests.exceptions.Timeout, requests.exceptions.ConnectionError):
            sleep_sec = 2 ** attempt
            time.sleep(sleep_sec)

    return None, "NETWORK_ERROR"


def enrich_with_omdb(movies_df, ratings_df, api_key=None, api_limit=900):
    """
    Enriches movies with OMDb metadata:
    - Prioritizes movies by total ratings count (most-rated first).
    - Skips movies already in local cache.
    - Limits new live calls to api_limit (default 900 to stay under 1,000/day limit).
    - Saves cache every 50 calls to avoid losing progress.
    - Only caches None for 'Movie not found'; never caches network or rate-limit errors.
    """
    logger.info("[Step 2/4] ENRICH: Fetching metadata from OMDb API (limit: %d calls)...", api_limit)
    cache = load_cache()

    # Prioritize popular movies by rating count
    rating_counts = ratings_df.groupby("movieId").size().to_dict()
    movies_df = movies_df.copy()
    movies_df["rating_count"] = movies_df["movieId"].map(rating_counts).fillna(0).astype(int)

    # Sort descending by rating count, then by movieId for consistency
    sorted_movies = movies_df.sort_values(by=["rating_count", "movieId"], ascending=[False, True])

    live_calls_made = 0
    cache_hits = 0
    new_not_found = 0
    api_limit_reached = False

    session = requests.Session()

    for row in sorted_movies.itertuples(index=False):
        movie_title = row.title
        imdb_id = getattr(row, "imdb_id", None)
        cache_key = imdb_id if (imdb_id and pd.notnull(imdb_id)) else movie_title

        # Skip if already cached
        if cache_key in cache:
            cache_hits += 1
            continue

        # Make new live API call if key is provided and limit not reached
        if api_key and live_calls_made < api_limit and not api_limit_reached:
            data, status = fetch_movie_from_api(
                api_key=api_key,
                imdb_id=imdb_id,
                title=movie_title,
                session=session,
            )
            live_calls_made += 1
            time.sleep(0.1)  # small pause between requests

            if data is not None:
                cache[cache_key] = data
            elif status == "NOT_FOUND":
                # Only cache negative result if movie was genuinely not found
                cache[cache_key] = None
                new_not_found += 1
            elif status == "LIMIT_REACHED":
                logger.warning("OMDb daily request limit reached. Stopping further calls.")
                api_limit_reached = True
            elif status == "INVALID_KEY":
                logger.warning("Invalid OMDb API key. Stopping further calls.")
                api_limit_reached = True

            # Save progress every 50 live calls
            if live_calls_made % 50 == 0:
                save_cache(cache)

    # Save final cache state
    save_cache(cache)
    logger.info("  - Cache hits: %d", cache_hits)
    logger.info("  - New live API calls made: %d", live_calls_made)
    logger.info("  - New confirmed 'not found' movies: %d", new_not_found)

    # Attach enriched fields back to movies_df
    directors = []
    plots = []
    box_offices = []
    runtimes = []

    for row in movies_df.itertuples(index=False):
        movie_title = row.title
        imdb_id = getattr(row, "imdb_id", None)
        cache_key = imdb_id if (imdb_id and pd.notnull(imdb_id)) else movie_title

        cached = cache.get(cache_key)
        if isinstance(cached, dict):
            directors.append(cached.get("Director"))
            plots.append(cached.get("Plot"))
            box_offices.append(cached.get("BoxOffice"))
            runtimes.append(cached.get("Runtime"))
        else:
            directors.append(None)
            plots.append(None)
            box_offices.append(None)
            runtimes.append(None)

    movies_df["director_raw"] = directors
    movies_df["plot_raw"] = plots
    movies_df["box_office_raw"] = box_offices
    movies_df["runtime_raw"] = runtimes

    # Print coverage count and percentage
    enriched_count = sum(
        1 for d, p, b, r in zip(directors, plots, box_offices, runtimes)
        if any(pd.notnull(v) and str(v).strip() not in ["N/A", "", "None"] for v in (d, p, b, r))
    )
    total_movies = len(movies_df)
    pct = (enriched_count / total_movies * 100) if total_movies > 0 else 0
    logger.info("  - Enrichment coverage: %d / %d movies (%.2f%%)", enriched_count, total_movies, pct)

    return movies_df


# -----------------------------------------------------------------------------
# Step 3: TRANSFORM
# -----------------------------------------------------------------------------
def transform_data(movies_df, ratings_df):
    """
    Cleans and transforms movie and rating data:
    1. Extracts release year and clean title from movie title strings.
    2. Derives decade column (Feature Engineering bonus).
    3. Cleans numeric types for Box Office and Runtime.
    4. Normalizes genres into distinct 'genres' and 'movie_genres' tables.
    5. Normalizes directors into distinct 'directors' and 'movie_directors' tables.
    6. Formats Unix rating timestamps into standard UTC datetime strings.
    """
    logger.info("[Step 3/4] TRANSFORM: Cleaning data and engineering features...")

    # 1. Title and Year parsing
    parsed_titles = [extract_year_and_clean_title(t) for t in movies_df["title"]]
    movies_df["release_year"] = [p[0] for p in parsed_titles]
    movies_df["clean_title"] = [p[1] for p in parsed_titles]

    # 2. Derive Decade
    movies_df["decade"] = [get_decade(y) for y in movies_df["release_year"]]

    # 3. Clean OMDb fields
    movies_df["box_office"] = [clean_box_office(b) for b in movies_df["box_office_raw"]]
    movies_df["runtime_minutes"] = [clean_runtime(r) for r in movies_df["runtime_raw"]]
    movies_df["plot"] = [clean_string(p) for p in movies_df["plot_raw"]]

    # Final cleaned movies DataFrame
    clean_movies_df = movies_df[[
        "movieId", "title", "clean_title", "release_year", "decade",
        "imdb_id", "plot", "box_office", "runtime_minutes"
    ]].rename(columns={"movieId": "movie_id"})

    # 4. Genre Normalization: split 'Action|Comedy' into rows
    unique_genres = set()
    movie_genre_pairs = []

    for row in movies_df.itertuples(index=False):
        m_id = row.movieId
        genre_str = getattr(row, "genres", "")
        if pd.notnull(genre_str) and genre_str != "(no genres listed)":
            for g in str(genre_str).split("|"):
                g_clean = g.strip()
                if g_clean:
                    unique_genres.add(g_clean)
                    movie_genre_pairs.append((m_id, g_clean))

    sorted_genres = sorted(list(unique_genres))
    genre_to_id = {name: idx + 1 for idx, name in enumerate(sorted_genres)}

    genres_df = pd.DataFrame([
        {"genre_id": genre_to_id[name], "genre_name": name}
        for name in sorted_genres
    ])

    movie_genres_df = pd.DataFrame([
        {"movie_id": m_id, "genre_id": genre_to_id[g_name]}
        for m_id, g_name in movie_genre_pairs
    ]).drop_duplicates()

    # 5. Director Normalization: split comma-separated directors into rows
    unique_directors = set()
    movie_director_pairs = []

    for row in movies_df.itertuples(index=False):
        m_id = row.movieId
        raw_director = getattr(row, "director_raw", None)
        directors_list = parse_directors(raw_director)
        for d in directors_list:
            unique_directors.add(d)
            movie_director_pairs.append((m_id, d))

    sorted_directors = sorted(list(unique_directors))
    director_to_id = {name: idx + 1 for idx, name in enumerate(sorted_directors)}

    directors_df = pd.DataFrame([
        {"director_id": director_to_id[name], "director_name": name}
        for name in sorted_directors
    ])

    movie_directors_df = pd.DataFrame([
        {"movie_id": m_id, "director_id": director_to_id[d_name]}
        for m_id, d_name in movie_director_pairs
    ]).drop_duplicates()

    # 6. Ratings Transformation: convert Unix timestamp to readable date string
    clean_ratings_df = ratings_df.dropna(subset=["userId", "movieId", "rating"]).copy()
    clean_ratings_df = clean_ratings_df.drop_duplicates(subset=["userId", "movieId"])
    clean_ratings_df["rated_at"] = pd.to_datetime(clean_ratings_df["timestamp"], unit="s").dt.strftime(
        "%Y-%m-%d %H:%M:%S"
    )
    clean_ratings_df = clean_ratings_df[["userId", "movieId", "rating", "rated_at"]].rename(
        columns={"userId": "user_id", "movieId": "movie_id"}
    )
    clean_ratings_df["rating_id"] = range(1, len(clean_ratings_df) + 1)

    logger.info("  - Extracted release years (found for %d movies)", clean_movies_df["release_year"].notnull().sum())
    logger.info("  - Normalized %d distinct genres and %d movie-genre links", len(genres_df), len(movie_genres_df))
    logger.info("  - Normalized %d distinct directors and %d movie-director links", len(directors_df), len(movie_directors_df))
    logger.info("  - Formatted %d ratings into ISO datetime strings", len(clean_ratings_df))

    return clean_movies_df, genres_df, movie_genres_df, directors_df, movie_directors_df, clean_ratings_df


# -----------------------------------------------------------------------------
# Step 4: LOAD & IDEMPOTENCY
# -----------------------------------------------------------------------------
def load_data_to_sqlite(
    movies_df,
    genres_df,
    movie_genres_df,
    directors_df,
    movie_directors_df,
    ratings_df,
    db_path=DB_FILE,
    schema_path=SCHEMA_SQL,
):
    """
    Loads transformed DataFrames into SQLite.
    Guarantees idempotency by:
    - Executing schema DDL with PRAGMA foreign_keys = ON.
    - Deleting from child tables first within a single atomic transaction.
    - Inserting records cleanly using fast itertuples execution.
    """
    logger.info("[Step 4/4] LOAD: Writing data to SQLite database (%s)...", os.path.basename(db_path))

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")

    if not os.path.exists(schema_path):
        conn.close()
        raise FileNotFoundError(f"Schema file not found at {schema_path}")

    with open(schema_path, "r", encoding="utf-8") as f:
        cursor.executescript(f.read())

    try:
        cursor.execute("BEGIN TRANSACTION;")
        # Delete from child tables first to respect foreign key constraints
        cursor.execute("DELETE FROM ratings;")
        cursor.execute("DELETE FROM movie_genres;")
        cursor.execute("DELETE FROM movie_directors;")
        cursor.execute("DELETE FROM genres;")
        cursor.execute("DELETE FROM directors;")
        cursor.execute("DELETE FROM movies;")

        # 1. Insert movies
        movie_rows = [
            (
                int(r.movie_id),
                r.title,
                r.clean_title,
                int(r.release_year) if pd.notnull(r.release_year) else None,
                r.decade,
                r.imdb_id,
                r.plot,
                int(r.box_office) if pd.notnull(r.box_office) else None,
                int(r.runtime_minutes) if pd.notnull(r.runtime_minutes) else None,
            )
            for r in movies_df.itertuples(index=False)
        ]
        cursor.executemany(
            """
            INSERT INTO movies (
                movie_id, title, clean_title, release_year, decade,
                imdb_id, plot, box_office, runtime_minutes
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
            """,
            movie_rows,
        )

        # 2. Insert genres
        genre_rows = [(int(r.genre_id), r.genre_name) for r in genres_df.itertuples(index=False)]
        cursor.executemany("INSERT INTO genres (genre_id, genre_name) VALUES (?, ?);", genre_rows)

        # 3. Insert movie_genres
        mg_rows = [(int(r.movie_id), int(r.genre_id)) for r in movie_genres_df.itertuples(index=False)]
        cursor.executemany("INSERT INTO movie_genres (movie_id, genre_id) VALUES (?, ?);", mg_rows)

        # 4. Insert directors
        director_rows = [(int(r.director_id), r.director_name) for r in directors_df.itertuples(index=False)]
        cursor.executemany("INSERT INTO directors (director_id, director_name) VALUES (?, ?);", director_rows)

        # 5. Insert movie_directors
        md_rows = [(int(r.movie_id), int(r.director_id)) for r in movie_directors_df.itertuples(index=False)]
        cursor.executemany("INSERT INTO movie_directors (movie_id, director_id) VALUES (?, ?);", md_rows)

        # 6. Insert ratings
        rating_rows = [
            (int(r.rating_id), int(r.user_id), int(r.movie_id), float(r.rating), r.rated_at)
            for r in ratings_df.itertuples(index=False)
        ]
        cursor.executemany(
            "INSERT INTO ratings (rating_id, user_id, movie_id, rating, rated_at) VALUES (?, ?, ?, ?, ?);",
            rating_rows,
        )

        conn.commit()
        logger.info("  [SUCCESS] Loaded %d records into 'movies'", len(movie_rows))
        logger.info("  [SUCCESS] Loaded %d records into 'genres'", len(genre_rows))
        logger.info("  [SUCCESS] Loaded %d records into 'movie_genres'", len(mg_rows))
        logger.info("  [SUCCESS] Loaded %d records into 'directors'", len(director_rows))
        logger.info("  [SUCCESS] Loaded %d records into 'movie_directors'", len(md_rows))
        logger.info("  [SUCCESS] Loaded %d records into 'ratings'", len(rating_rows))

    except Exception as e:
        conn.rollback()
        logger.error("Database load failed: %s", e)
        raise e
    finally:
        conn.close()


# -----------------------------------------------------------------------------
# Data Integrity Validation
# -----------------------------------------------------------------------------
def validate_database(db_path=DB_FILE, expected_movies_count=0, expected_ratings_count=0):
    """
    Validates data integrity post-load:
    - Asserts row counts match expected source counts.
    - Asserts no orphan ratings exist (ratings.movie_id not in movies).
    - Asserts no duplicate (user_id, movie_id) exists in ratings.
    - Reports missing release_year count.
    """
    logger.info("Performing post-load data integrity checks...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # 1. Check movie count
    cursor.execute("SELECT COUNT(*) FROM movies;")
    actual_movies = cursor.fetchone()[0]
    assert actual_movies == expected_movies_count, (
        f"Validation Error: Expected {expected_movies_count} movies, found {actual_movies}"
    )

    # 2. Check rating count
    cursor.execute("SELECT COUNT(*) FROM ratings;")
    actual_ratings = cursor.fetchone()[0]
    assert actual_ratings == expected_ratings_count, (
        f"Validation Error: Expected {expected_ratings_count} ratings, found {actual_ratings}"
    )

    # 3. Check for orphan ratings
    cursor.execute(
        """
        SELECT COUNT(*) FROM ratings r
        LEFT JOIN movies m ON r.movie_id = m.movie_id
        WHERE m.movie_id IS NULL;
        """
    )
    orphan_ratings = cursor.fetchone()[0]
    assert orphan_ratings == 0, f"Validation Error: Found {orphan_ratings} orphan ratings!"

    # 4. Check for duplicate (user_id, movie_id)
    cursor.execute(
        """
        SELECT COUNT(*) FROM (
            SELECT user_id, movie_id, COUNT(*)
            FROM ratings
            GROUP BY user_id, movie_id
            HAVING COUNT(*) > 1
        );
        """
    )
    duplicate_ratings = cursor.fetchone()[0]
    assert duplicate_ratings == 0, f"Validation Error: Found {duplicate_ratings} duplicate rating pairs!"

    # 5. Missing release_year count
    cursor.execute("SELECT COUNT(*) FROM movies WHERE release_year IS NULL;")
    missing_years = cursor.fetchone()[0]
    logger.info("  [PASSED] Row count matches source (%d movies, %d ratings)", actual_movies, actual_ratings)
    logger.info("  [PASSED] No orphan ratings found")
    logger.info("  [PASSED] No duplicate user-movie ratings found")
    logger.info("  [INFO]   Movies with missing release year: %d", missing_years)

    conn.close()


# -----------------------------------------------------------------------------
# Single Source of Truth Query Verification
# -----------------------------------------------------------------------------
def parse_queries_file(queries_path=QUERIES_SQL):
    """
    Reads queries.sql and splits it into individual queries based on '-- [Question'.
    This ensures queries.sql is the single source of truth for both SQL and Python.
    """
    if not os.path.exists(queries_path):
        raise FileNotFoundError(f"Queries file not found at {queries_path}")

    with open(queries_path, "r", encoding="utf-8") as f:
        content = f.read()

    queries = []
    # Split by '-- [Question' to get each question block
    parts = content.split("-- [Question")
    for part in parts[1:]:  # skip any text before the first question
        full_block = "-- [Question" + part
        lines = full_block.strip().splitlines()
        title = lines[0].strip()  # the first line has the question title

        # Filter out comment lines and empty lines to extract pure SQL
        sql_lines = [l for l in lines[1:] if l.strip() and not l.strip().startswith("--")]
        sql = "\n".join(sql_lines).strip()
        if sql:
            queries.append((title, sql))
    return queries


def run_analytical_queries(db_path=DB_FILE, queries_path=QUERIES_SQL):
    """
    Executes all queries from queries.sql against the database and prints results.
    """
    print("\n" + "=" * 70)
    print("ANALYTICAL QUERY RESULTS (Single Source of Truth: queries.sql)")
    print("=" * 70)

    queries = parse_queries_file(queries_path)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    for header, sql in queries:
        print(f"\n>>> {header}")
        try:
            cursor.execute(sql)
            rows = cursor.fetchall()
            headers = [desc[0] for desc in cursor.description] if cursor.description else []

            if not rows:
                print("    (No rows returned)")
                continue

            # Format rows nicely
            if len(rows) > 30:
                # Print full table for year query
                col_widths = [max(len(h), 12) for h in headers]
                header_line = " | ".join(h.ljust(w) for h, w in zip(headers, col_widths))
                separator_line = "-+-".join("-" * w for w in col_widths)
                print("    " + header_line)
                print("    " + separator_line)
                for r in rows:
                    row_line = " | ".join(str(val).ljust(w) for val, w in zip(r, col_widths))
                    print("    " + row_line)
                print(f"    (Total rows: {len(rows)})")
            else:
                for idx, r in enumerate(rows, 1):
                    row_str = ", ".join(f"{col}: {val}" for col, val in zip(headers, r))
                    print(f"    {idx}. {row_str}")

        except Exception as e:
            print(f"    [ERROR executing query]: {e}")

    conn.close()
    print("\n" + "=" * 70)


# -----------------------------------------------------------------------------
# Main Execution Entrypoint
# -----------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Movie Data Pipeline ETL Script")
    parser.add_argument(
        "--api-key",
        type=str,
        default=os.getenv("OMDB_API_KEY"),
        help="OMDb API Key (priority: CLI flag > environment variable > .env file)",
    )
    parser.add_argument(
        "--api-limit",
        type=int,
        default=900,
        help="Maximum new live OMDb API requests per run (default: 900 to stay under daily quota)",
    )
    parser.add_argument(
        "--skip-analytics",
        action="store_true",
        help="Skip printing analytical query results at the end",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Movie Data Pipeline - ETL Execution")
    print("=" * 60)

    if not args.api_key or args.api_key == "your_key_here":
        logger.info("No active OMDb API key provided. Using existing local cache.")
        logger.info("To enable live enrichment, set OMDB_API_KEY in .env or pass --api-key <key>")
        active_api_key = None
    else:
        active_api_key = args.api_key.strip()

    # 1. Extract
    movies_df, ratings_df = extract_csv_data()

    # 2. Enrich
    enriched_movies_df = enrich_with_omdb(
        movies_df=movies_df,
        ratings_df=ratings_df,
        api_key=active_api_key,
        api_limit=args.api_limit,
    )

    # 3. Transform
    clean_movies, clean_genres, clean_mg, clean_directors, clean_md, clean_ratings = transform_data(
        enriched_movies_df, ratings_df
    )

    # 4. Load
    load_data_to_sqlite(
        clean_movies,
        clean_genres,
        clean_mg,
        clean_directors,
        clean_md,
        clean_ratings,
    )

    # 5. Validate Integrity
    validate_database(
        db_path=DB_FILE,
        expected_movies_count=len(clean_movies),
        expected_ratings_count=len(clean_ratings),
    )

    # 6. Verify Analytics
    if not args.skip_analytics:
        run_analytical_queries(db_path=DB_FILE, queries_path=QUERIES_SQL)

    logger.info("ETL Pipeline completed successfully!")


if __name__ == "__main__":
    main()
