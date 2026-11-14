-- ============================================================================
-- Analytical SQL Queries
-- Assignment: Building a Simple Movie Data Pipeline (tsworks)
-- Database: SQLite (movies.db)
-- ============================================================================

-- [Question 1] Which movie has the highest average rating? (Qualified: >= 50 ratings)
-- Assumption: Minimum 50 ratings required to filter out obscure movies with 1 or 2 lucky 5.0 ratings.
SELECT 
    m.title,
    ROUND(AVG(r.rating), 2) AS average_rating,
    COUNT(r.rating) AS total_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
HAVING COUNT(r.rating) >= 50
ORDER BY average_rating DESC, total_ratings DESC, m.title ASC
LIMIT 1;

-- [Question 1 - Option A (Strict)] Absolute highest average rating without vote threshold
-- Strict mathematical interpretation with deterministic tie-breaking.
SELECT 
    m.title,
    ROUND(AVG(r.rating), 2) AS average_rating,
    COUNT(r.rating) AS total_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
GROUP BY m.movie_id, m.title
ORDER BY average_rating DESC, total_ratings DESC, m.title ASC
LIMIT 1;

-- [Question 2] Top 5 movie genres with highest average rating (Weighted per rating)
-- Joins ratings through movie_genres to genres. Average is weighted across all ratings in each genre.
SELECT 
    g.genre_name,
    ROUND(AVG(r.rating), 2) AS average_rating,
    COUNT(r.rating) AS total_ratings
FROM genres g
JOIN movie_genres mg ON g.genre_id = mg.genre_id
JOIN ratings r ON mg.movie_id = r.movie_id
GROUP BY g.genre_id, g.genre_name
ORDER BY average_rating DESC, total_ratings DESC, g.genre_name ASC
LIMIT 5;

-- [Question 3] Director with the most movies in this dataset (Top with ties)
-- Joins directors and movie_directors. Uses a subquery to return all directors tied for first place.
WITH director_counts AS (
    SELECT 
        d.director_id,
        d.director_name,
        COUNT(DISTINCT md.movie_id) AS movie_count
    FROM directors d
    JOIN movie_directors md ON d.director_id = md.director_id
    GROUP BY d.director_id, d.director_name
)
SELECT 
    director_name,
    movie_count
FROM director_counts
WHERE movie_count = (SELECT MAX(movie_count) FROM director_counts)
ORDER BY director_name ASC;

-- [Question 4] Average rating of movies released each year
-- Groups movies by release year and orders them chronologically from earliest to latest.
SELECT 
    m.release_year,
    ROUND(AVG(r.rating), 2) AS average_rating,
    COUNT(r.rating) AS total_ratings
FROM movies m
JOIN ratings r ON m.movie_id = r.movie_id
WHERE m.release_year IS NOT NULL
GROUP BY m.release_year
ORDER BY m.release_year ASC;
