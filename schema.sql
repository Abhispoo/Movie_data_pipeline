-- ============================================================================
-- Database Schema: Movie Data Pipeline
-- Relational Database: SQLite
-- ============================================================================

-- Enable foreign key support in SQLite
PRAGMA foreign_keys = ON;

-- ----------------------------------------------------------------------------
-- 1. Table: movies
-- Stores core movie details from MovieLens enriched with OMDb API data.
-- Director data is normalized into separate directors and movie_directors tables.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS movies (
    movie_id        INTEGER PRIMARY KEY,
    title           TEXT NOT NULL,
    clean_title     TEXT,
    release_year    INTEGER,
    decade          TEXT,
    imdb_id         TEXT,
    plot            TEXT,
    box_office      INTEGER,
    runtime_minutes INTEGER
);

-- ----------------------------------------------------------------------------
-- 2. Table: genres
-- Lookup table for distinct movie genres.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS genres (
    genre_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    genre_name  TEXT UNIQUE NOT NULL
);

-- ----------------------------------------------------------------------------
-- 3. Table: movie_genres
-- Junction (bridge) table representing the Many-to-Many relationship
-- between movies and genres.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS movie_genres (
    movie_id    INTEGER NOT NULL,
    genre_id    INTEGER NOT NULL,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES movies (movie_id) ON DELETE CASCADE,
    FOREIGN KEY (genre_id) REFERENCES genres (genre_id) ON DELETE CASCADE
);

-- ----------------------------------------------------------------------------
-- 4. Table: directors
-- Lookup table for distinct movie directors (split from multi-director strings).
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS directors (
    director_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    director_name  TEXT UNIQUE NOT NULL
);

-- ----------------------------------------------------------------------------
-- 5. Table: movie_directors
-- Junction (bridge) table representing the Many-to-Many relationship
-- between movies and directors.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS movie_directors (
    movie_id       INTEGER NOT NULL,
    director_id    INTEGER NOT NULL,
    PRIMARY KEY (movie_id, director_id),
    FOREIGN KEY (movie_id) REFERENCES movies (movie_id) ON DELETE CASCADE,
    FOREIGN KEY (director_id) REFERENCES directors (director_id) ON DELETE CASCADE
);

-- ----------------------------------------------------------------------------
-- 6. Table: ratings
-- Stores individual user ratings for movies.
-- rating_id uses INTEGER PRIMARY KEY without AUTOINCREMENT so IDs remain stable,
-- and UNIQUE(user_id, movie_id) guarantees no duplicate user-movie ratings.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ratings (
    rating_id   INTEGER PRIMARY KEY,
    user_id     INTEGER NOT NULL,
    movie_id    INTEGER NOT NULL,
    rating      REAL NOT NULL,
    rated_at    TEXT NOT NULL,
    UNIQUE(user_id, movie_id),
    FOREIGN KEY (movie_id) REFERENCES movies (movie_id) ON DELETE CASCADE
);

-- ----------------------------------------------------------------------------
-- Indexes for query performance optimization
-- ----------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_movies_release_year ON movies(release_year);
CREATE INDEX IF NOT EXISTS idx_ratings_movie_id ON ratings(movie_id);
CREATE INDEX IF NOT EXISTS idx_movie_genres_genre_id ON movie_genres(genre_id);
CREATE INDEX IF NOT EXISTS idx_movie_directors_director_id ON movie_directors(director_id);
