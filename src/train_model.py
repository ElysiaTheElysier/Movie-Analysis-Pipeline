import pandas as pd
import numpy as np
import joblib
import urllib
import sys
import warnings
import os
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import r2_score, mean_squared_error
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import StackingRegressor, GradientBoostingRegressor
from sklearn.linear_model import LinearRegression
import xgboost as xgb
import lightgbm as lgb
import config
from utils import analyze_risk_and_safety 
import logging
from datetime import datetime

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
MODELS_DIR = os.path.join(PROJECT_ROOT, 'models')
DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'AI_Training_Data.csv')
LOG_DIR = os.path.join(CURRENT_DIR, 'logs')

if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(LOG_DIR, f"training_{datetime.now():%Y%m%d}.log")),
        logging.StreamHandler()
    ]
)
warnings.filterwarnings("ignore")

DB_CONN_STR = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config.SERVER_NAME};DATABASE=Movie_DB;Trusted_Connection=yes;"

os.makedirs(MODELS_DIR, exist_ok=True)

MODEL_PATH = os.path.join(MODELS_DIR, 'film_revenue_v25.pkl')
VECTORIZER_PATH = os.path.join(MODELS_DIR, 'tfidf_v25.pkl')
FEATURES_PATH = os.path.join(MODELS_DIR, 'features_v25.pkl')
KNOWLEDGE_PATH = os.path.join(MODELS_DIR, 'knowledge_v25.pkl')

logging.info(f"Directory check: Saving models to -> {MODELS_DIR}")

def build_knowledge_base():
    logging.info("Loading knowledge base from SQL Server...")
    try:
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(DB_CONN_STR)}")

        query = """
        SELECT c.name, c.role, AVG(CAST(f.revenue AS FLOAT)) as avg_revenue, COUNT(f.movie_id) as movie_count
        FROM Bridge_Credits c JOIN Fact_Financials f ON c.movie_id = f.movie_id
        WHERE f.revenue > 1000000 
        GROUP BY c.name, c.role
        """
        df_stats = pd.read_sql(query, engine)
        
        knowledge = {
            'Actor': df_stats[df_stats['role'] == 'Actor'].set_index('name').to_dict('index'),
            'Director': df_stats[df_stats['role'] == 'Director'].set_index('name').to_dict('index')
        }
        global_stats = {
            'avg_revenue': df_stats['avg_revenue'].mean(),
            'max_revenue': df_stats['avg_revenue'].max()
        }
        return knowledge, global_stats
    except Exception as e:
        logging.critical(f"SQL Error: {e}")
        sys.exit(1)

def get_training_data():
    logging.info("Processing data...")
    try:
        df = pd.read_csv(DATA_PATH)
        logging.info(f"Training data loaded successfully from CSV: {df.shape[0]} records.")
    except Exception as e:
        logging.critical(f"Critical error reading '{DATA_PATH}'. Please run feature_engineering.py first! Error: {e}")
        sys.exit(1)
    
    if 'month' not in df.columns:
        logging.error("Missing 'month' column in CSV. Re-run feature_engineering.py.")
        sys.exit(1)
    
    df['is_summer'] = df['month'].apply(lambda x: 1 if x in [5,6,7,8] else 0)
    df['is_holiday'] = df['month'].apply(lambda x: 1 if x in [11,12] else 0)
    
    try:
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(DB_CONN_STR)}")
        df_over = pd.read_sql("SELECT movie_id, overview FROM Dim_Movie_Rich_Info", engine)
        df = df.merge(df_over, on='movie_id', how='left').fillna({'overview': ''})
    except:
        df['overview'] = ""

    df = df[df['revenue'] > 1000000].copy() 
    
    df['revenue_log'] = np.log1p(df['revenue'])
    df['budget_log'] = np.log1p(df['budget'])
    df['cast_power'] = np.log1p(df['cast_power'])
    df['director_power'] = np.log1p(df['director_power'])
    
    df['budget_x_franchise'] = df['budget_log'] * df['is_franchise'] 
    df['budget_x_cast'] = df['budget_log'] * df['cast_power']
    df['cast_x_director'] = df['cast_power'] * df['director_power']
    
    return df

def train_final_model():
    logging.info("Starting Model Training...")
    
    knowledge, global_stats = build_knowledge_base()
    df = get_training_data()
    
    logging.info("NLP Processing...")
    tfidf = TfidfVectorizer(stop_words='english', max_features=5000, ngram_range=(1, 2))
    svd = TruncatedSVD(n_components=30, random_state=42)
    nlp_pipe = make_pipeline(tfidf, svd)
    
    vecs = nlp_pipe.fit_transform(df['overview'])
    nlp_cols = [f'nlp_{i}' for i in range(30)]
    df_nlp = pd.DataFrame(vecs, columns=nlp_cols, index=df.index)
    df = pd.concat([df, df_nlp], axis=1)
    
    features = ['budget_log', 'runtime', 'cast_power', 'director_power', 
                'budget_x_cast', 'budget_x_franchise', 'cast_x_director',
                'is_franchise', 'rating_score', 'month', 'is_summer', 'is_holiday'] + \
               [c for c in df.columns if 'genre_' in c] + nlp_cols

    logging.info("Splitting data (85% Train - 15% Test)...")
    X = df[features]
    y = df['revenue_log']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, random_state=42)
    
    logging.info("Training Stacking Model...")
    gb_reg = GradientBoostingRegressor(n_estimators=1500, learning_rate=0.02, max_depth=5, random_state=42)
    xgb_reg = xgb.XGBRegressor(n_estimators=1500, learning_rate=0.02, max_depth=6, n_jobs=-1, random_state=42)
    lgb_reg = lgb.LGBMRegressor(n_estimators=1500, learning_rate=0.02, num_leaves=35, verbose=-1, n_jobs=-1, random_state=42)

    stacking = StackingRegressor(
        estimators=[('gb', gb_reg), ('xgb', xgb_reg), ('lgb', lgb_reg)], 
        final_estimator=LinearRegression(), 
        n_jobs=-1 
    )
    
    stacking.fit(X_train, y_train)
    y_pred = stacking.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    
    logging.info("-" * 30)
    logging.info(f"Model Accuracy (R2): {r2:.4f}")
    logging.info(f"RMSE Error: {rmse:.4f}")
    logging.info("-" * 30)

    logging.info("Retraining on full dataset...")
    stacking.fit(X, y)
    
    logging.info("Saving artifacts...")
    joblib.dump(stacking, MODEL_PATH)
    joblib.dump(nlp_pipe, VECTORIZER_PATH)
    joblib.dump(features, FEATURES_PATH)
    joblib.dump((knowledge, global_stats), KNOWLEDGE_PATH)
    
    logging.info("Training complete. Files saved successfully.")
    logging.info(f"-> {MODEL_PATH}")

if __name__ == "__main__":
    train_final_model()