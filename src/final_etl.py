import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine, text
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import config

API_KEY = config.TMDB_API_KEY
SERVER = config.SERVER_NAME
DB = "Movie_DB"
WORKERS = 20
SESSION = requests.Session()

def get_db():
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DB};Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

def init_db():
    engine = get_db()
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
    print("Database is ready.")

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
            conn.execute(text(f"DROP TABLE {temp_table}"))
    except Exception as e:
        print(f"Could not update {table_name}: {e}")

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
        pass 

def fetch_movie_details(mid):
    url = f"https://api.themoviedb.org/3/movie/{mid}?api_key={API_KEY}&append_to_response=credits,release_dates"
    try:
        r = SESSION.get(url, timeout=5)
        if r.status_code != 200: return None
        d = r.json()
        
        if d.get('vote_count', 0) < 5 and d.get('revenue', 0) == 0: return None

        rating = 'NR'
        for country in d.get('release_dates', {}).get('results', []):
            if country['iso_3166_1'] == 'US':
                for release in country['release_dates']:
                    if release['certification']:
                        rating = release['certification']
                        break
        
        studio = d['production_companies'][0]['name'] if d.get('production_companies') else None
        franchise = d['belongs_to_collection']['name'] if d.get('belongs_to_collection') else 'Stand-alone'

        dim_movie = {
            'movie_id': d['id'],
            'title': d['title'],
            'release_date': d.get('release_date'),
            'year': int(d['release_date'][:4]) if d.get('release_date') else None,
            'popularity': d.get('popularity'),
            'vote_average': d.get('vote_average'),
            'vote_count': d.get('vote_count'),
            'original_language': d.get('original_language'),
            'overview': d.get('overview', '')[:4000]
        }
        fact_fin = {
            'movie_id': d['id'],
            'budget': d.get('budget', 0),
            'revenue': d.get('revenue', 0),
            'profit': d.get('revenue', 0) - d.get('budget', 0)
        }
        rich_info = {
            'movie_id': d['id'],
            'collection_name': franchise,
            'production_company': studio,
            'mpaa_rating': rating,
            'runtime': d.get('runtime', 0),
            'tagline': d.get('tagline', ''),
            'overview': d.get('overview', '')[:4000]
        }
        
        credits_data = []
        c = d.get('credits', {})
        for crew in c.get('crew', []):
            if crew['job'] == 'Director':
                credits_data.append({
                    'credit_unique_id': f"{d['id']}_DIR_{crew['id']}",
                    'movie_id': d['id'], 'person_id': crew['id'], 'name': crew['name'], 'role': 'Director', 'item_order': 0
                })
                break
        for actor in c.get('cast', [])[:8]:
            credits_data.append({
                'credit_unique_id': f"{d['id']}_ACT_{actor['id']}",
                'movie_id': d['id'], 'person_id': actor['id'], 'name': actor['name'], 'role': 'Actor', 'item_order': actor['order']
            })
            
        genres_data = [{'movie_id': d['id'], 'genre_name': g['name']} for g in d.get('genres', [])]

        return {'dim': dim_movie, 'fact': fact_fin, 'rich': rich_info, 'credits': credits_data, 'genres': genres_data}
    except: return None

def run():
    print(f"Starting the process at {datetime.now()}...")
    init_db()
    eng = get_db()
    
    candidates = set()
    years = range(2000, 2027) 
    print("Searching for movies on TMDB...")
    
    for year in years:
        strategies = [('popularity.desc', 5), ('revenue.desc', 3), ('vote_count.desc', 2)]
        for sort_by, pages in strategies:
            for p in range(1, pages + 1):
                try:
                    u = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&sort_by={sort_by}&primary_release_year={year}&vote_count.gte=5&page={p}"
                    res = SESSION.get(u).json().get('results', [])
                    for i in res: candidates.add(i['id'])
                except: continue
                
    print(f"Found {len(candidates)} movies to download.")

    batch_size = 200
    buffer = {'dim': [], 'fact': [], 'rich': [], 'credits': [], 'genres': []}
    
    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futures = {ex.submit(fetch_movie_details, mid): mid for mid in candidates}
        
        for idx, f in enumerate(as_completed(futures)):
            res = f.result()
            if res:
                buffer['dim'].append(res['dim'])
                buffer['fact'].append(res['fact'])
                buffer['rich'].append(res['rich'])
                buffer['credits'].extend(res['credits'])
                buffer['genres'].extend(res['genres'])
            
            if (idx + 1) % batch_size == 0 or (idx + 1) == len(futures):
                print(f"Saving batch {idx+1} of {len(candidates)}...")
                
                df_dim = pd.DataFrame(buffer['dim'])
                df_fact = pd.DataFrame(buffer['fact'])
                df_rich = pd.DataFrame(buffer['rich'])
                df_cred = pd.DataFrame(buffer['credits'])
                df_gen = pd.DataFrame(buffer['genres'])

                robust_upsert(df_dim, 'Dim_Movies', ['movie_id'], eng)
                robust_upsert(df_fact, 'Fact_Financials', ['movie_id'], eng)
                robust_upsert(df_rich, 'Dim_Movie_Rich_Info', ['movie_id'], eng)
                robust_upsert(df_cred, 'Bridge_Credits', ['credit_unique_id'], eng)
                upsert_genres(df_gen, eng)
                
                buffer = {'dim': [], 'fact': [], 'rich': [], 'credits': [], 'genres': []}

    print(f"Done. Finished at {datetime.now()}.")

if __name__ == "__main__":
    run()