# Movie Data Pipeline

## 1. Overview
This project is an ETL data pipeline built in Python and SQLite for the tsworks Data Engineering assessment. It extracts movie metadata and user ratings from the MovieLens dataset, enriches the most popular movies with plot, runtime, box office, and director information from the external OMDb API, cleans and normalizes the data into a relational database, and runs SQL queries to answer four analytical questions. The pipeline is designed to be fully idempotent and self-validating.

---

## 2. Setup and Run

### Prerequisites
- Python 3.8 or higher (tested on Python 3.13)
- `pip` package manager

### Installation
Clone the repository and install the dependencies:
```bash
git clone <your-repo-link>
cd movie_data_pipeline
pip install -r requirements.txt
```

### OMDb API Key Setup
The project includes a pre-seeded local cache (`omdb_cache.json`) for 161 popular movies so the pipeline runs out of the box without requiring an API key.

If you have an OMDb API key:
1. Create a `.env` file by copying `.env.example`:
   ```bash
   cp .env.example .env
   ```
2. Set your key:
   ```text
   OMDB_API_KEY=your_key_here
   ```
*(Key resolution priority: `--api-key` CLI flag > OS environment variable > `.env` file).*

### Running the Pipeline
Run the main script:
```bash
python etl.py
```
This script reads the CSVs, enriches records via cache/API, loads the normalized tables into `movies.db` (which is created automatically), verifies data integrity, and prints the answers to all four analytical queries directly from `queries.sql`.

Optional flags:
- `--api-key <key>`: Pass an OMDb API key directly.
- `--api-limit <N>`: Maximum new live API calls per run (default: 900, staying within OMDb's 1,000/day free limit).
- `--skip-analytics`: Skip printing analytical query results at the end.

### How to Resume Enrichment on Later Days
Because free OMDb keys allow 1,000 requests per 24-hour window, enriching all 9,742 movies requires multiple runs:
- The script automatically sorts movies by popularity (`rating_count DESC`) and skips movies already saved in `omdb_cache.json`.
- Running `python etl.py` on Day 1 enriches up to 900 new movies and saves them to `omdb_cache.json`.
- Running `python etl.py` on Day 2 automatically picks up the next 900 un-enriched movies, gradually filling the cache without duplicating requests.

### Running Unit Tests
```bash
pytest
```

---

## 3. Repository Structure

```text
movie_data_pipeline/
├── data/
│   ├── movies.csv          # MovieLens movies (9,742 records)
│   ├── ratings.csv         # MovieLens user ratings (100,836 records)
│   └── links.csv           # MovieLens IMDb/TMDb identifier mappings
├── tests/
│   └── test_etl.py         # Pytest suite for cleaning, parsing, and idempotency
├── omdb_cache.json         # Local cache for OMDb API responses
├── schema.sql              # Relational database DDL and indexes
├── etl.py                  # Python ETL pipeline script
├── queries.sql             # Single source of truth for analytical SQL queries
├── pytest.ini              # Pytest configuration
├── requirements.txt        # Pinned Python dependencies
├── .env.example            # Environment configuration template
├── .gitignore              # Ignores .env, *.db, and cache directories
└── README.md               # Project documentation
```

---

## 4. Data Model

The SQLite database (`movies.db`) follows Third Normal Form (3NF) to eliminate redundancy and support clean joins.

```text
┌──────────────────┐           ┌──────────────────┐           ┌──────────────────┐
│      movies      │ 1       * │   movie_genres   │ *       1 │      genres      │
├──────────────────┤───────────├──────────────────┤───────────├──────────────────┤
│ movie_id (PK)    │           │ movie_id (PK,FK) │           │ genre_id (PK)    │
│ title            │           │ genre_id (PK,FK) │           │ genre_name (UQ)  │
│ clean_title      │           └──────────────────┘           └──────────────────┘
│ release_year     │
│ decade           │           ┌──────────────────┐           ┌──────────────────┐
│ imdb_id          │ 1       * │ movie_directors  │ *       1 │    directors     │
│ plot             │───────────├──────────────────┤───────────├──────────────────┤
│ box_office       │           │ movie_id (PK,FK) │           │ director_id (PK) │
│ runtime_minutes  │           │ director_id(PK,FK│           │ director_name(UQ)│
└──────────────────┘           └──────────────────┘           └──────────────────┘
         │ 1
         │
         │ *
┌──────────────────┐
│     ratings      │
├──────────────────┤
│ rating_id (PK)   │
│ user_id          │
│ movie_id (FK)    │
│ rating           │
│ rated_at         │
│ UNIQUE(user,mov) │
└──────────────────┘
```

- **Genres Bridge Table (`genres`, `movie_genres`)**: In `movies.csv`, genres are pipe-separated strings (`Action|Adventure`). Storing multiple values in one column violates First Normal Form (1NF). Splitting them into a bridge table enables standard `GROUP BY` and indexing.
- **Directors Bridge Table (`directors`, `movie_directors`)**: OMDb returns multiple directors as a comma-separated string. Normalizing directors into a bridge table avoids fragile string matching and allows exact counting of distinct movies per director.
- **Ratings Table**: Uses `rating_id INTEGER PRIMARY KEY` (avoiding SQLite `AUTOINCREMENT` sequence growth across reloads) with an explicit `UNIQUE(user_id, movie_id)` constraint to prevent duplicate user reviews.
- **Indexes**: Created on foreign key join columns and analytical query columns: `movies(release_year)`, `ratings(movie_id)`, `movie_genres(genre_id)`, and `movie_directors(director_id)`.

---

## 5. Design Choices and Assumptions

1. **Exact API Lookups via `links.csv`**: Querying OMDb by title can return incorrect remakes or fail on punctuation. I mapped each MovieLens movie to its exact IMDb ID from `links.csv` (`tt0114709`) and queried OMDb by ID (`i=tt...`) for 100% accuracy.
2. **Threshold Assumption for Question 1 (>= 50 ratings)**: Without a vote threshold, obscure movies with only 1 or 2 ratings of 5.0 rank first. I established a minimum threshold of 50 ratings as a practical analytical assumption, while keeping the strict unfiltered result as a documented alternative.
3. **Genre Rating Averages (Question 2)**: The genre average is computed as a weighted average across all individual ratings cast in that genre.
4. **Partial Enrichment Strategy**: Because free OMDb keys are capped at 1,000 requests per day, I prioritized the most-rated movies first. This ensures our analytical queries have rich metadata for the dataset's most significant films, while un-enriched movies retain `NULL` for external fields without breaking the pipeline.

---

## 6. Challenges and How I Solved Them

1. **OMDb Rate Limits and Cache Poisoning**: If an API call fails due to rate limits or invalid keys, caching `None` would permanently mark that movie as missing. I built error handling to only cache negative results when OMDb explicitly returns `"Movie not found!"`. If `"Request limit reached!"` occurs, the script immediately halts further calls.
2. **Multiple Directors in One String**: OMDb often returns co-directors together (e.g. `"Joel Coen, Ethan Coen"`). I wrote `parse_directors()` to tokenize and trim names into distinct rows in `directors` and `movie_directors`.
3. **Idempotency**: Running an ingestion script multiple times often causes duplicate key errors or duplicate rows. I wrapped the load step in a single atomic database transaction (`BEGIN TRANSACTION ... COMMIT`) that deletes records from child tables first before parent tables, then repopulates them cleanly.
4. **Missing Release Years**: 13 movies in MovieLens lack a year tag in parentheses in their title string. My extraction regex extracts 4-digit years where present and leaves missing years as `NULL`, which Question 4 filters out cleanly.

---

## 7. Results

These results are generated directly from `movies.db` by executing `queries.sql`:

### Enrichment Summary
- **Enriched Movies in Cache**: 161 / 9,742 (1.65%)
- **Distinct Normalized Directors**: 127
- **Distinct Normalized Genres**: 19
- **Total Ingested Ratings**: 100,836

### Question 1: Which movie has the highest average rating?
- **Qualified Result (Minimum 50 ratings threshold)**:
  - **Title**: *The Shawshank Redemption (1994)*
  - **Average Rating**: 4.43 / 5.0
  - **Total Ratings**: 317
- **Strict Option A (Unfiltered with deterministic tie-break)**:
  - **Title**: *Belle époque (1992)*
  - **Average Rating**: 5.0 / 5.0
  - **Total Ratings**: 2

### Question 2: Top 5 movie genres with highest average rating (Weighted per rating)
| Rank | Genre Name | Average Rating | Total Ratings Count |
| :---: | :--- | :---: | :---: |
| 1 | Film-Noir | 3.92 | 870 |
| 2 | War | 3.81 | 4,859 |
| 3 | Documentary | 3.80 | 1,219 |
| 4 | Drama | 3.66 | 41,928 |
| 5 | Crime | 3.66 | 16,681 |

*(Note: Drama and Crime both round to 3.66; Drama ranks higher due to having 41,928 ratings versus Crime's 16,681).*

### Question 3: Director with the most movies in this dataset
- **Top Director**: **Steven Spielberg** with **10 movies**
- **Filmography in Enriched Sample**:
  1. *Jaws (1975)*
  2. *Raiders of the Lost Ark (1981)*
  3. *E.T. the Extra-Terrestrial (1982)*
  4. *Indiana Jones and the Temple of Doom (1984)*
  5. *Indiana Jones and the Last Crusade (1989)*
  6. *Jurassic Park (1993)*
  7. *Schindler's List (1993)*
  8. *Saving Private Ryan (1998)*
  9. *Minority Report (2002)*
  10. *Catch Me If You Can (2002)*
- *Coverage Note*: This result reflects the most represented director within the currently enriched 161 popular movies. Because the script prioritizes the most-rated films first, Spielberg naturally leads this group. As more movies are enriched across daily runs, this query will evaluate across the full movie catalog.

### Question 4: Average rating of movies released each year
Sample summary across chronological release periods:
| Period | Sample Year | Average Rating | Total Ratings Count |
| :---: | :---: | :---: | :---: |
| Early Cinema | 1920 | 3.81 | 8 |
| Golden Age | 1942 | 3.92 | 185 |
| Classic Hollywood | 1957 | 4.04 | 215 |
| New Hollywood | 1975 | 3.88 | 637 |
| Modern Era | 1994 | 3.50 | 5,296 |
| Recent Cinema | 2017 | 3.58 | 461 |

*(Full chronological table from 1902 to 2018 spanning 106 years is printed during pipeline execution).*

---

## 8. Limitations and Future Improvements

1. **Full Catalog Enrichment**: The primary limitation is OMDb's 1,000 request daily rate limit. Future iterations could use official bulk IMDb non-commercial dataset dumps to enrich all 9,742 movies in a single offline step.
2. **Cloud Scaling (Azure + Snowflake)**:
   - Ingest raw CSVs and API JSON payloads into **Azure Data Lake Storage Gen2 (ADLS Gen2)** in a raw storage zone.
   - Schedule and orchestrate daily ingestion pipelines using **Azure Data Factory (ADF)**.
   - Load transformed records into **Snowflake** tables (`DIM_MOVIES`, `DIM_GENRES`, `DIM_DIRECTORS`, `FACT_RATINGS`) using `MERGE INTO` statements for incremental, idempotent upserts.
3. **CI/CD Automation**: Add GitHub Actions workflow to run `pytest` and linter checks automatically on every commit.
