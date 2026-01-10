import requests
import pandas as pd
import time
import os
import urllib
from sqlalchemy import create_engine
from datetime import datetime
import config

# --- CONFIGURATION ---
API_KEY = config.TMDB_API_KEY
SERVER_NAME = config.SERVER_NAME
DATABASE_NAME = "Movie_DB"

DEFAULT_START_YEAR = 2000
END_YEAR = 2026
PAGES_PER_YEAR = 25
MIN_VOTE_COUNT = 100

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
BACKUP_DIR = os.path.join(os.path.dirname(CURRENT_DIR), 'Data_Backup')

def get_sql_conn():
    """Create SQL Server connection"""
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        f"Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def get_latest_year_from_db():
    """Get the max release year currently in database for incremental load"""
    try:
        engine = get_sql_conn()
        query = "SELECT MAX(YEAR(release_date)) FROM Fact_Movies"
        result = pd.read_sql(query, engine)
        max_year = result.iloc[0, 0]
        
        if max_year and max_year >= DEFAULT_START_YEAR:
            return int(max_year)
        return DEFAULT_START_YEAR
    except Exception:
        return DEFAULT_START_YEAR

def run_pipeline():
    print(f"[{datetime.now()}] STARTING ETL PIPELINE")
    
    # 1. Determine start year
    start_year = get_latest_year_from_db()
    print(f"Incremental Load: Starting fetch from year {start_year}")

    all_movies = []

    # 2. Extract Data
    for year in range(start_year, END_YEAR + 1):
        print(f"Fetching year {year}...", end=" ", flush=True)
        
        for page in range(1, PAGES_PER_YEAR + 1):
            url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&sort_by=popularity.desc&primary_release_year={year}&vote_count.gte={MIN_VOTE_COUNT}&page={page}"
            
            try:
                response = requests.get(url, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    movies = data.get('results', [])
                    
                    for m in movies:
                        all_movies.append({
                            'movie_id': m['id'],
                            'title': m['title'],
                            'release_date': m.get('release_date', ''),
                            'popularity': m.get('popularity', 0),
                            'vote_average': m.get('vote_average', 0),
                            'vote_count': m.get('vote_count', 0),
                            'overview': m.get('overview', ''),
                            'original_language': m.get('original_language', ''),
                            'genre_ids': str(m.get('genre_ids', []))
                        })
                time.sleep(0.05) # Rate limiting
                
            except Exception as e:
                print(f"[Error: {e}]", end=" ")
                continue
        print("Done.")

    if not all_movies:
        print("No new data found.")
        return

    # 3. Transform Data
    print("Processing data...")
    df = pd.DataFrame(all_movies)
    
    # Deduplicate based on movie_id
    df.drop_duplicates(subset=['movie_id'], inplace=True)
    
    # Format dates
    df['release_date'] = pd.to_datetime(df['release_date'], errors='coerce')
    
    print(f"Total records to load: {len(df)}")

    # 4. Load Data
    # 4.1 Save Backup CSV
    if not os.path.exists(BACKUP_DIR):
        os.makedirs(BACKUP_DIR)
    
    csv_file = f"movies_full_data_{datetime.now().strftime('%Y%m%d')}.csv"
    df.to_csv(os.path.join(BACKUP_DIR, csv_file), index=False, encoding='utf-8-sig')
    print(f"Backup saved: {csv_file}")
    
    # 4.2 Save to SQL Server
    try:
        engine = get_sql_conn()
        df.to_sql('Fact_Movies', con=engine, if_exists='append', index=False)
        print("Successfully loaded into SQL Server.")
    except Exception as e:
        print(f"Database Load Error: {e}")
        if "PRIMARY KEY" in str(e):
            print("Hint: Duplicates found. Ignore if using basic append.")

    print(f"[{datetime.now()}] ETL COMPLETED")

if __name__ == "__main__":
    run_pipeline()