import pandas as pd
import numpy as np
import urllib
from sqlalchemy import create_engine
import config

# --- KẾT NỐI ---
SERVER = config.SERVER_NAME
DB = "Movie_DB"
params = urllib.parse.quote_plus(f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={SERVER};DATABASE={DB};Trusted_Connection=yes;")
engine = create_engine(f"mssql+pyodbc:///?odbc_connect={params}")

# --- 1. BẢNG CPI MỸ (2000-2025) ---
# Nguồn: US Bureau of Labor Statistics (CPI-U Annual Average)
# Base Year: 2024 (CPI ~ 314.0)
CPI_DATA = {
    2000: 172.2, 2001: 177.1, 2002: 179.9, 2003: 184.0, 2004: 188.9,
    2005: 195.3, 2006: 201.6, 2007: 207.3, 2008: 215.3, 2009: 214.5,
    2010: 218.1, 2011: 224.9, 2012: 229.6, 2013: 233.0, 2014: 236.7,
    2015: 237.0, 2016: 240.0, 2017: 245.1, 2018: 251.1, 2019: 255.7,
    2020: 258.8, 2021: 271.0, 2022: 292.7, 2023: 304.7, 2024: 314.0,
    2025: 322.0 # Dự báo
}

def adjust_for_inflation(amount, year):
    """Quy đổi tiền từ năm X về giá trị năm 2024"""
    if year not in CPI_DATA or amount is None: return amount
    cpi_year = CPI_DATA[year]
    cpi_2024 = CPI_DATA[2024]
    return amount * (cpi_2024 / cpi_year)

def load_data():
    print("[INFO] Loading raw data from SQL Server...")
    query_movies = """
    SELECT 
        m.movie_id, m.title, m.release_date, m.year,
        f.budget, f.revenue,
        r.runtime, r.mpaa_rating, r.collection_name, r.production_company
    FROM Dim_Movies m
    JOIN Fact_Financials f ON m.movie_id = f.movie_id
    JOIN Dim_Movie_Rich_Info r ON m.movie_id = r.movie_id
    WHERE m.release_date IS NOT NULL 
      AND f.budget > 1000 
    ORDER BY m.release_date ASC
    """
    df_movies = pd.read_sql(query_movies, engine)
    
    # --- BƯỚC MỚI: ĐIỀU CHỈNH LẠM PHÁT ---
    print("[INFO] Adjusting Budget & Revenue for Inflation (Base 2024)...")
    
    # Tạo cột mới để so sánh (Giữ cột gốc để check nếu cần)
    df_movies['budget_adj'] = df_movies.apply(lambda x: adjust_for_inflation(x['budget'], x['year']), axis=1)
    df_movies['revenue_adj'] = df_movies.apply(lambda x: adjust_for_inflation(x['revenue'], x['year']), axis=1)
    
    # Thay thế cột chính bằng giá trị đã điều chỉnh để Model học cái này
    df_movies['budget_raw'] = df_movies['budget'] # Lưu lại bản gốc
    df_movies['revenue_raw'] = df_movies['revenue']
    df_movies['budget'] = df_movies['budget_adj']
    df_movies['revenue'] = df_movies['revenue_adj']
    
    print(f"   Example: $1 in 2000 -> ${adjust_for_inflation(1, 2000):.2f} in 2024")

    df_credits = pd.read_sql("SELECT movie_id, person_id, role, item_order FROM Bridge_Credits", engine)
    df_genres = pd.read_sql("SELECT movie_id, genre_name FROM Bridge_Movie_Genres", engine)
    
    return df_movies, df_credits, df_genres

def calculate_rolling_features(df_movies, df_credits):
    print("[INFO] Starting Time-Travel Feature Engineering...")
    person_history = {} 
    cast_power_list = []
    director_power_list = []
    
    credits_group = df_credits.groupby('movie_id')
    
    for idx, row in df_movies.iterrows():
        mid = row['movie_id']
        # LƯU Ý: Ở đây ta dùng 'revenue' đã điều chỉnh lạm phát
        revenue = row['revenue'] 
        
        current_cast_power = 0
        current_dir_power = 0
        
        if mid in credits_group.groups:
            crew_data = credits_group.get_group(mid)
            
            # Actor Power
            actors = crew_data[crew_data['role'] == 'Actor'].sort_values('item_order').head(3)
            actor_revs = []
            for pid in actors['person_id']:
                if pid in person_history:
                    history = person_history[pid]
                    avg_rev = np.mean(history[-5:]) if history else 0
                    actor_revs.append(avg_rev)
            current_cast_power = np.mean(actor_revs) if actor_revs else 0
            
            # Director Power
            director = crew_data[crew_data['role'] == 'Director'].head(1)
            for pid in director['person_id']:
                if pid in person_history:
                    history = person_history[pid]
                    current_dir_power = np.mean(history[-5:]) if history else 0

            # Cập nhật lịch sử (Dùng doanh thu ĐÃ CHỈNH lạm phát để tích lũy uy tín)
            if revenue > 0:
                for pid in crew_data['person_id']:
                    if pid not in person_history: person_history[pid] = []
                    person_history[pid].append(revenue)
        
        cast_power_list.append(current_cast_power)
        director_power_list.append(current_dir_power)
        
        if idx % 2000 == 0: print(f"   Processed {idx}/{len(df_movies)} movies...")

    df_movies['cast_power'] = cast_power_list
    df_movies['director_power'] = director_power_list
    return df_movies

def run_pipeline():
    df, credits, genres = load_data()
    df = calculate_rolling_features(df, credits)
    
    print("[INFO] Encoding Genres...")
    genres_pivot = pd.crosstab(genres['movie_id'], genres['genre_name']).add_prefix('genre_')
    df = df.merge(genres_pivot, on='movie_id', how='left').fillna(0)
    
    print("[INFO] Encoding Metadata...")
    df['is_franchise'] = df['collection_name'].apply(lambda x: 1 if x and x != 'Stand-alone' else 0)
    rating_map = {'G': 0, 'PG': 1, 'PG-13': 2, 'R': 3, 'NC-17': 4, 'NR': 2}
    df['rating_score'] = df['mpaa_rating'].map(rating_map).fillna(2)
    
    output_file = 'AI_Training_Data.csv'
    # Bỏ cột raw, chỉ giữ cột đã adjust để train
    final_df = df.drop(columns=['collection_name', 'mpaa_rating', 'production_company', 'budget_raw', 'revenue_raw', 'budget_adj', 'revenue_adj']) 
    final_df.to_csv(output_file, index=False)
    
    print(f"\n[SUCCESS] Feature Engineering Complete (Inflation Adjusted)!")
    print(f"-> Saved to '{output_file}'")

if __name__ == "__main__":
    run_pipeline()
    