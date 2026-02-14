import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import config
import logging
import os
from requests.adapters import HTTPAdapter

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_DIR = os.path.join(CURRENT_DIR, 'logs')

if not os.path.exists(LOG_DIR):
    os.makedirs(LOG_DIR)

log_filename = os.path.join(LOG_DIR, f"etl_process_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")

logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s | %(levelname)-8s | %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_filename, encoding='utf-8'), 
        logging.StreamHandler() 
    ]
)

logging.info("Logging system initialized.")

API_KEY = config.TMDB_API_KEY
SERVER = config.SERVER_NAME
DB = "Movie_DB"
WORKERS = 40
SESSION = requests.Session()

adapter = HTTPAdapter(pool_connections=WORKERS, pool_maxsize=WORKERS, max_retries=3)
SESSION.mount('https://', adapter)
SESSION.mount('http://', adapter)

def get_db():
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DB};Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

def init_db():
    engine = get_db()
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Dim_Movie_Rich_Info' AND xtype='U')
                CREATE TABLE Dim_Movie_Rich_Info (
                    movie_id INT PRIMARY KEY,
                    collection_name NVARCHAR(255),
                    production_company NVARCHAR(255),
                    mpaa_rating NVARCHAR(50),
                    runtime INT,
                    tagline NVARCHAR(MAX),
                    overview NVARCHAR(MAX)
                )
            """))
            
            conn.execute(text("""
                IF COL_LENGTH('Dim_Movie_Rich_Info', 'production_company') IS NULL
                BEGIN
                    ALTER TABLE Dim_Movie_Rich_Info ADD production_company NVARCHAR(255);
                END
            """))
            conn.execute(text("""
                IF COL_LENGTH('Dim_Movie_Rich_Info', 'mpaa_rating') IS NULL
                BEGIN
                    ALTER TABLE Dim_Movie_Rich_Info ADD mpaa_rating NVARCHAR(50);
                END
            """))

            conn.execute(text("""
                IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='Bridge_Credits' AND xtype='U')
                CREATE TABLE Bridge_Credits (
                    credit_unique_id NVARCHAR(100) PRIMARY KEY,
                    movie_id INT,
                    person_id INT,
                    name NVARCHAR(255),
                    role NVARCHAR(50),
                    item_order INT
                )
            """))
        logging.info("Database is ready.")
    except Exception as e:
        logging.error(f"Database initialization failed: {e}")

def robust_upsert(df, table_name, pk_cols, engine):
    if df.empty: return
    
    df = df.drop_duplicates(subset=pk_cols, keep='first')

    temp_table = f"Temp_{table_name}_{int(datetime.now().timestamp())}"
    try:
        df.to_sql(temp_table, engine, if_exists='replace', index=False)
        cols = [c for c in df.columns]
        
        on_clause = " AND ".join([f"target.{c} = source.{c}" for c in pk_cols])
        update_set = ", ".join([f"target.{c} = source.{c}" for c in cols if c not in pk_cols])
        insert_cols = ", ".join(cols)
        insert_vals = ", ".join([f"source.{c}" for c in cols])
        
        when_matched = f"WHEN MATCHED THEN UPDATE SET {update_set}" if update_set else ""
        
        sql = f"""
            MERGE INTO {table_name} AS target
            USING {temp_table} AS source
            ON ({on_clause})
            {when_matched}
            WHEN NOT MATCHED THEN
                INSERT ({insert_cols}) VALUES ({insert_vals});
        """
        with engine.begin() as conn:
            conn.execute(text(sql))

    except Exception as e:
        logging.error(f"CRITICAL ERROR: Cannot Update {table_name}. Details: {e}")
    finally: 
        try:
            with engine.begin() as conn:
                conn.execute(text(f"IF OBJECT_ID('{temp_table}', 'U') IS NOT NULL DROP TABLE {temp_table}"))
        except Exception as e:
            logging.error(f"Failed to drop temp table {temp_table}: {e}")

def upsert_genres(df, engine):
    if df.empty: return
    df = df.drop_duplicates()
    
    temp_table = f"Temp_Genres_{int(datetime.now().timestamp())}"
    try:
        df.to_sql(temp_table, engine, if_exists='replace', index=False)
        sql = f"""
            INSERT INTO Bridge_Movie_Genres (movie_id, genre_name)
            SELECT DISTINCT t.movie_id, t.genre_name
            FROM {temp_table} t
            WHERE NOT EXISTS (
                SELECT 1 FROM Bridge_Movie_Genres b 
                WHERE b.movie_id = t.movie_id AND b.genre_name = t.genre_name
            )
        """
        with engine.begin() as conn:
            conn.execute(text(sql))
            conn.execute(text(f"DROP TABLE {temp_table}"))
    except Exception as e:
        logging.error(f"Error when Upsert Genres: {e}")

def fetch_candidate_page(url):
    try:
        res = SESSION.get(url, timeout=10)
        if res.status_code == 200:
            return [i['id'] for i in res.json().get('results', [])]
        else:
            logging.error(f"API Error {res.status_code} for URL: {url}")
            return []
    except requests.exceptions.Timeout:
        logging.error(f"Timeout gathering candidate URL: {url}")
        return []
    except Exception as e:
        logging.error(f"Exception gathering candidate URL {url}: {e}")
        return []

def fetch_movie_details(mid):
    url = f"https://api.themoviedb.org/3/movie/{mid}?api_key={API_KEY}&append_to_response=credits,release_dates"
    try:
        r = SESSION.get(url, timeout=10)
        if r.status_code != 200: 
            logging.warning(f"Failed to fetch details for MID {mid}. Status: {r.status_code}")
            return None
        d = r.json()
        
        release_date = d.get('release_date', '')
        if not release_date: 
            return None
        
        year = int(release_date[:4])
        budget = d.get('budget', 0)
        revenue = d.get('revenue', 0)
        runtime = d.get('runtime', 0)
        overview = d.get('overview', '')

        if year < 2000 or year > 2025: 
            return None
        if budget < 100000 or revenue < 10000: 
            return None
        if runtime < 60 or runtime > 240: 
            return None
        if not overview or len(overview.strip()) < 50: 
            return None

        rating = 'NR'
        for country in d.get('release_dates', {}).get('results', []):
            if country['iso_3166_1'] == 'US':
                for release in country['release_dates']:
                    if release['certification']:
                        rating = release['certification']
                        break
        
        studio = d['production_companies'][0]['name'] if d.get('production_companies') else None
        franchise = d['belongs_to_collection']['name'] if d.get('belongs_to_collection') else 'Stand-alone'

        c = d.get('credits', {})
        crew = c.get('crew', [])
        cast = c.get('cast', [])
        
        if len(cast) < 3: 
            return None
        
        has_director = False
        for person in crew:
            if person['job'] == 'Director':
                has_director = True
                break
                
        if not has_director: 
            return None

        dim_movie = {
            'movie_id': d['id'],
            'title': d['title'],
            'release_date': release_date,
            'year': year,
            'popularity': d.get('popularity'),
            'vote_average': d.get('vote_average'),
            'vote_count': d.get('vote_count'),
            'original_language': d.get('original_language'),
            'overview': overview[:4000]
        }
        fact_fin = {
            'movie_id': d['id'],
            'budget': budget,
            'revenue': revenue,
            'profit': revenue - budget
        }
        rich_info = {
            'movie_id': d['id'],
            'collection_name': franchise,
            'production_company': studio,
            'mpaa_rating': rating,
            'runtime': runtime,
            'tagline': d.get('tagline', ''),
            'overview': overview[:4000]
        }
        
        credits_data = []
        for person in crew:
            if person['job'] == 'Director':
                credits_data.append({
                    'credit_unique_id': f"{d['id']}_DIR_{person['id']}",
                    'movie_id': d['id'], 'person_id': person['id'], 'name': person['name'], 'role': 'Director', 'item_order': 0
                })
                break
                
        for actor in cast[:8]:
            credits_data.append({
                'credit_unique_id': f"{d['id']}_ACT_{actor['id']}",
                'movie_id': d['id'], 'person_id': actor['id'], 'name': actor['name'], 'role': 'Actor', 'item_order': actor['order']
            })
            
        genres_data = [{'movie_id': d['id'], 'genre_name': g['name']} for g in d.get('genres', [])]

        return {'dim': dim_movie, 'fact': fact_fin, 'rich': rich_info, 'credits': credits_data, 'genres': genres_data}
    except requests.exceptions.Timeout:
        logging.error(f"Timeout fetching details for MID {mid}")
        return None
    except Exception as e:
        logging.error(f"Exception fetching details for MID {mid}: {e}")
        return None

def run():
    logging.info(f"Starting the process at {datetime.now()}...")
    init_db()
    eng = get_db()
    
    candidates = set()
    years = range(2000, 2026) 
    strategies = [('revenue.desc', 50), ('popularity.desc', 50), ('vote_count.desc', 20)]
    
    candidate_urls = []
    for year in years:
        for sort_by, pages in strategies:
            for p in range(1, pages + 1):
                u = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&sort_by={sort_by}&primary_release_year={year}&vote_count.gte=10&page={p}"
                candidate_urls.append(u)

    logging.info(f"Generated {len(candidate_urls)} URLs to scan for candidates. Starting parallel scan...")

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        future_to_url = {ex.submit(fetch_candidate_page, url): url for url in candidate_urls}
        for idx, future in enumerate(as_completed(future_to_url)):
            if (idx + 1) % 500 == 0:
                logging.info(f"Scanned {idx + 1}/{len(candidate_urls)} candidate pages...")
            try:
                ids = future.result()
                candidates.update(ids)
            except Exception as e:
                logging.error(f"Error processing candidate future: {e}")
                
    logging.info(f"Found {len(candidates)} unique movies to download. Proceeding to detail extraction...")

    batch_size = 200
    buffer = {'dim': [], 'fact': [], 'rich': [], 'credits': [], 'genres': []}
    
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_movie_details, mid): mid for mid in candidates}
        
        for idx, f in enumerate(as_completed(futures)):
            if (idx + 1) % 500 == 0:
                logging.info(f"Processed details for {idx + 1}/{len(candidates)} movies...")
            
            try:
                res = f.result()
                if res:
                    buffer['dim'].append(res['dim'])
                    buffer['fact'].append(res['fact'])
                    buffer['rich'].append(res['rich'])
                    buffer['credits'].extend(res['credits'])
                    buffer['genres'].extend(res['genres'])
            except Exception as e:
                logging.error(f"Error extracting future detail result: {e}")
            
            if (idx + 1) % batch_size == 0 or (idx + 1) == len(futures):
                logging.info(f"Saving batch ending at index {idx+1}...")
                try:
                    robust_upsert(pd.DataFrame(buffer['dim']), 'Dim_Movies', ['movie_id'], eng)
                    robust_upsert(pd.DataFrame(buffer['fact']), 'Fact_Financials', ['movie_id'], eng)
                    robust_upsert(pd.DataFrame(buffer['rich']), 'Dim_Movie_Rich_Info', ['movie_id'], eng)
                    robust_upsert(pd.DataFrame(buffer['credits']), 'Bridge_Credits', ['credit_unique_id'], eng)
                    upsert_genres(pd.DataFrame(buffer['genres']), eng)
                except Exception as e:
                    logging.error(f"Failed to save batch ending at index {idx+1}: {e}")
                
                buffer = {'dim': [], 'fact': [], 'rich': [], 'credits': [], 'genres': []}
                
    if len(buffer['dim']) > 0:
        logging.info("Saving final residual batch...")
        try:
            robust_upsert(pd.DataFrame(buffer['dim']), 'Dim_Movies', ['movie_id'], eng)
            robust_upsert(pd.DataFrame(buffer['fact']), 'Fact_Financials', ['movie_id'], eng)
            robust_upsert(pd.DataFrame(buffer['rich']), 'Dim_Movie_Rich_Info', ['movie_id'], eng)
            robust_upsert(pd.DataFrame(buffer['credits']), 'Bridge_Credits', ['credit_unique_id'], eng)
            upsert_genres(pd.DataFrame(buffer['genres']), eng)
        except Exception as e:
            logging.error(f"Failed to save final residual batch: {e}")

    logging.info("ETL Pipeline Finished")

if __name__ == "__main__":
    run()