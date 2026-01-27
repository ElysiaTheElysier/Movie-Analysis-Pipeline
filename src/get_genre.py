import requests
import pandas as pd
import urllib
from sqlalchemy import create_engine
from datetime import datetime
import config


API_KEY = config.TMDB_API_KEY
SERVER_NAME = config.SERVER_NAME
DATABASE_NAME = "Movie_DB"

def get_sql_conn():
    params = urllib.parse.quote_plus(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={SERVER_NAME};"
        f"DATABASE={DATABASE_NAME};"
        f"Trusted_Connection=yes;"
    )
    return create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

def fetch_genres():
    print(f"[{datetime.now()}] STARTING GENRE ETL")
    print("Fetching genre list from TMDB API...", end=" ")
    
    url = f"https://api.themoviedb.org/3/genre/movie/list?api_key={API_KEY}&language=en-US"
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            genres = response.json().get('genres', [])
            df = pd.DataFrame(genres)
            df.columns = ['genre_id', 'genre_name'] 
            
            # Load to DB
            engine = get_sql_conn()
            df.to_sql('Dim_Genres', con=engine, if_exists='replace', index=False)
            print(f"Done.\nSuccessfully loaded {len(df)} genres into [Dim_Genres].")
        else:
            print(f"Failed. Status code: {response.status_code}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    fetch_genres()