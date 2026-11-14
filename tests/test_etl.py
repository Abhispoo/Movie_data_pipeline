"""
Unit and integration tests for the Movie Data Pipeline.
Run with: pytest
"""

import sys
from pathlib import Path
import sqlite3
import pandas as pd
import pytest

# Ensure repository root is on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from etl import (
    extract_year_and_clean_title,
    get_decade,
    clean_box_office,
    clean_runtime,
    parse_directors,
    load_data_to_sqlite,
    SCHEMA_SQL,
)


# Proves that 4-digit release year in parentheses is extracted and stripped from title
def test_extract_year_and_clean_title():
    year, title = extract_year_and_clean_title("Toy Story (1995)")
    assert year == 1995
    assert title == "Toy Story"

    # Title without year should return None for year and keep original title
    year, title = extract_year_and_clean_title("Apollo 13")
    assert year is None
    assert title == "Apollo 13"


# Proves that a release year is correctly converted into a decade string
def test_get_decade():
    assert get_decade(1995) == "1990s"
    assert get_decade(2018) == "2010s"
    assert get_decade(None) is None


# Proves that box office strings with '$' and ',' convert to clean integers
def test_clean_box_office():
    assert clean_box_office("$373,554,033") == 373554033
    assert clean_box_office("N/A") is None
    assert clean_box_office(None) is None


# Proves that runtime strings like '81 min' convert to integer minutes
def test_clean_runtime():
    assert clean_runtime("81 min") == 81
    assert clean_runtime("N/A") is None
    assert clean_runtime(None) is None


# Proves that comma-separated director strings split into a clean list of individual names
def test_parse_directors():
    assert parse_directors("Joel Coen, Ethan Coen") == ["Joel Coen", "Ethan Coen"]
    assert parse_directors("Steven Spielberg") == ["Steven Spielberg"]
    assert parse_directors("N/A") == []


# Proves that running database load multiple times is idempotent with identical row counts
def test_idempotent_load(tmp_path):
    db_file = str(tmp_path / "test_movies.db")

    sample_movies = pd.DataFrame([
        {
            "movie_id": 1,
            "title": "Toy Story (1995)",
            "clean_title": "Toy Story",
            "release_year": 1995,
            "decade": "1990s",
            "imdb_id": "tt0114709",
            "plot": "A cowboy doll...",
            "box_office": 373554033,
            "runtime_minutes": 81,
        },
        {
            "movie_id": 2,
            "title": "Jumanji (1995)",
            "clean_title": "Jumanji",
            "release_year": 1995,
            "decade": "1990s",
            "imdb_id": "tt0113497",
            "plot": "A magical game...",
            "box_office": 262797249,
            "runtime_minutes": 104,
        },
    ])

    sample_genres = pd.DataFrame([
        {"genre_id": 1, "genre_name": "Adventure"},
        {"genre_id": 2, "genre_name": "Animation"},
    ])

    sample_mg = pd.DataFrame([
        {"movie_id": 1, "genre_id": 1},
        {"movie_id": 1, "genre_id": 2},
        {"movie_id": 2, "genre_id": 1},
    ])

    sample_directors = pd.DataFrame([
        {"director_id": 1, "director_name": "John Lasseter"},
        {"director_id": 2, "director_name": "Joe Johnston"},
    ])

    sample_md = pd.DataFrame([
        {"movie_id": 1, "director_id": 1},
        {"movie_id": 2, "director_id": 2},
    ])

    sample_ratings = pd.DataFrame([
        {"rating_id": 1, "user_id": 101, "movie_id": 1, "rating": 5.0, "rated_at": "2020-01-01 12:00:00"},
        {"rating_id": 2, "user_id": 102, "movie_id": 1, "rating": 4.0, "rated_at": "2020-01-02 12:00:00"},
        {"rating_id": 3, "user_id": 101, "movie_id": 2, "rating": 3.5, "rated_at": "2020-01-03 12:00:00"},
    ])

    # First load
    load_data_to_sqlite(
        movies_df=sample_movies,
        genres_df=sample_genres,
        movie_genres_df=sample_mg,
        directors_df=sample_directors,
        movie_directors_df=sample_md,
        ratings_df=sample_ratings,
        db_path=db_file,
        schema_path=SCHEMA_SQL,
    )

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM movies;")
    assert cur.fetchone()[0] == 2
    cur.execute("SELECT COUNT(*) FROM ratings;")
    assert cur.fetchone()[0] == 3
    cur.execute("SELECT COUNT(*) FROM movie_directors;")
    assert cur.fetchone()[0] == 2
    conn.close()

    # Second load (verifying idempotency)
    load_data_to_sqlite(
        movies_df=sample_movies,
        genres_df=sample_genres,
        movie_genres_df=sample_mg,
        directors_df=sample_directors,
        movie_directors_df=sample_md,
        ratings_df=sample_ratings,
        db_path=db_file,
        schema_path=SCHEMA_SQL,
    )

    conn = sqlite3.connect(db_file)
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM movies;")
    assert cur.fetchone()[0] == 2
    cur.execute("SELECT COUNT(*) FROM ratings;")
    assert cur.fetchone()[0] == 3
    cur.execute("SELECT COUNT(*) FROM movie_directors;")
    assert cur.fetchone()[0] == 2
    conn.close()
