import requests
import pandas as pd
import urllib
import time
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import config


API_KEY = config.TMDB_API_KEY
SERVER_NAME = config.SERVER_NAME
DATABASE_NAME = "Movie_DB"

MAX_WORKERS = 8       
BATCH_SIZE = 1000    

def get_sql_conn():
    """Create SQL Server connection with fast_executemany enabled"""
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        f"Trusted_Connection=yes;"
    )

    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}", fast_executemany=True)

def fetch_movie_details(movie_id):
    """Worker function to fetch details for a single movie"""
    url = f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={API_KEY}&language=en-US"
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            m = response.json()
            return {
                'movie_id': m['id'],
                'budget': m.get('budget', 0),
                'revenue': m.get('revenue', 0),
                'runtime': m.get('runtime', 0),
                'status': m.get('status', ''),
                'tagline': m.get('tagline', '')
            }
    except Exception:
        return None
    return None

def enrich_data():
    print(f"[{datetime.now()}] STARTING FINANCIAL DATA ENRICHMENT")
    
    engine = get_sql_conn()
    
    # 1. Identify missing data
    print("Identifying missing records...", end=" ", flush=True)
    try:
        query_all = "SELECT movie_id FROM Fact_Movies"
        all_ids = set(pd.read_sql(query_all, engine)['movie_id'])
        
        # Check if table exists
        try:
            query_done = "SELECT movie_id FROM Fact_Financials"
            done_ids = set(pd.read_sql(query_done, engine)['movie_id'])
        except:
            done_ids = set()
            
        todo_ids = list(all_ids - done_ids)
        print(f"Done. Found {len(todo_ids)} movies to process.")
        
    except Exception as e:
        print(f"\nError reading DB: {e}")
        return

    if not todo_ids:
        print("All data is up to date.")
        return

    # 2. Multithreading Execution
    print(f"Fetching data with {MAX_WORKERS} workers...")
    
    batch_data = []
    total_processed = 0
    total_tasks = len(todo_ids)

    # Use ThreadPoolExecutor to run tasks in parallel
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Submit all tasks
        future_to_id = {executor.submit(fetch_movie_details, mid): mid for mid in todo_ids}
        
        for future in as_completed(future_to_id):
            result = future.result()
            if result:
                batch_data.append(result)
            
            total_processed += 1
            
            # Progress Log
            if total_processed % 100 == 0:
                print(f"-> Progress: {total_processed}/{total_tasks}...", end='\r')

            # 3. Batch Insert (Save every BATCH_SIZE items)
            if len(batch_data) >= BATCH_SIZE:
                df_batch = pd.DataFrame(batch_data)
                df_batch.to_sql('Fact_Financials', con=engine, if_exists='append', index=False)
                print(f"\n Saved batch of {len(df_batch)} records.")
                batch_data = [] # Reset batch

    # 4. Save remaining data
    if batch_data:
        df_batch = pd.DataFrame(batch_data)
        df_batch.to_sql('Fact_Financials', con=engine, if_exists='append', index=False)
        print(f"\nSaved final batch of {len(df_batch)} records.")

    print(f"[{datetime.now()}] FINANCIAL ENRICHMENT COMPLETED")

if __name__ == "__main__":
    enrich_data()