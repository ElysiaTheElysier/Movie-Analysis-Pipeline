import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import config

# Config
API_KEY = config.TMDB_API_KEY
SERVER = config.SERVER_NAME
DB = "Movie_DB"
THRESHOLD = 10000 
WORKERS = 20
SESSION = requests.Session()

def get_db():
    # SQL Server connection string
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DB};Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

def get_existing():
    # Fetch existing IDs to avoid duplication
    try:
        return set(pd.read_sql("SELECT movie_id FROM Dim_Movies", get_db())['movie_id'])
    except:
        return set()

def fetch_data(mid):
    url = f"https://api.themoviedb.org/3/movie/{mid}?api_key={API_KEY}"
    try:
        r = SESSION.get(url, timeout=4)
        if r.status_code != 200: return None
        d = r.json()
        
        rev = d.get('revenue', 0)
        bud = d.get('budget', 0)
        ov = d.get('overview', '')

        # Filter: Ignore movies with no revenue or description
        if rev < 1: return None
        if not ov or len(ov) < 10: return None

        # 1. Dim_Movies
        movie_info = {
            'movie_id': d['id'],
            'title': d['title'],
            'release_date': d.get('release_date'),
            'year': int(d['release_date'][:4]) if d.get('release_date') else None,
            'popularity': d.get('popularity'),
            'vote_average': d.get('vote_average'),
            'vote_count': d.get('vote_count'),
            'original_language': d.get('original_language')
        }

        # 2. Fact_Financials
        financial_info = {
            'movie_id': d['id'],
            'budget': bud,
            'revenue': rev,
            'profit': rev - bud,
            'roi': (rev - bud) / bud if bud > 0 else 0
        }

        # 3. Bridge_Movie_Genres (Normalization)
        genre_list = []
        for g in d.get('genres', []):
            genre_list.append({
                'movie_id': d['id'],
                'genre_name': g['name']
            })

        return {
            'dim': movie_info,
            'fact': financial_info,
            'bridge': genre_list
        }
    except:
        return None

def run():
    print(f"[{datetime.now()}] Starting Star Schema ETL process...")
    eng = get_db()
    
    # --- SCHEMA RESET (Updated for SQLAlchemy 2.0+) ---
    # Uncomment lines below for the first run or to reset DB
    #with eng.connect() as conn:
     #   print("Resetting tables...")
     #   conn.execute(text("DROP TABLE IF EXISTS Bridge_Movie_Genres"))
     #   conn.execute(text("DROP TABLE IF EXISTS Fact_Financials"))
     #   conn.execute(text("DROP TABLE IF EXISTS Dim_Movies"))
     #   conn.commit()
    
    exist = get_existing()
    print(f"Existing records: {len(exist)}")

    # Strategy selection based on data volume
    if len(exist) < THRESHOLD:
        print("Mode: FULL LOAD (Deep Scan)")
        years = range(2000, 2027)
        pages = 50 
        strats = ['revenue.desc']
    else:
        print("Mode: INCREMENTAL LOAD (Latest updates)")
        years = [2025, 2026]
        pages = 5
        strats = ['release_date.desc']

    batch_dim, batch_fact, batch_bridge = [], [], []
    
    for y in years:
        print(f"Processing year: {y}")
        candidates = set()
        
        # 1. ID Discovery
        for s in strats:
            for p in range(1, pages + 1):
                try:
                    u = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&sort_by={s}&primary_release_year={y}&vote_count.gte=10&page={p}"
                    res = SESSION.get(u, timeout=3).json().get('results', [])
                    if not res: break
                    for x in res:
                        if x['id'] not in exist: candidates.add(x['id'])
                except: continue
        
        if not candidates: continue
        print(f"-> Found {len(candidates)} new candidates. Fetching details...")

        # 2. Parallel Extraction
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(fetch_data, i) for i in candidates]
            for f in as_completed(futs):
                r = f.result()
                if r:
                    batch_dim.append(r['dim'])
                    batch_fact.append(r['fact'])
                    batch_bridge.extend(r['bridge'])
                    exist.add(r['dim']['movie_id'])

        # 3. Batch Load to SQL
        if batch_dim:
            pd.DataFrame(batch_dim).to_sql('Dim_Movies', eng, if_exists='append', index=False)
            pd.DataFrame(batch_fact).to_sql('Fact_Financials', eng, if_exists='append', index=False)
            pd.DataFrame(batch_bridge).to_sql('Bridge_Movie_Genres', eng, if_exists='append', index=False)
            print(f"   Committed: {len(batch_dim)} movies, {len(batch_bridge)} genre records.")
            batch_dim, batch_fact, batch_bridge = [], [], [] # Clear buffer

    print("ETL pipeline completed successfully.")

if __name__ == "__main__":
    run()