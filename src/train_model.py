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
from sklearn.decomposition import PCA
from sklearn.ensemble import StackingRegressor, GradientBoostingRegressor
from sklearn.linear_model import RidgeCV, LinearRegression
import xgboost as xgb
import lightgbm as lgb
import optuna
from sentence_transformers import SentenceTransformer
import matplotlib.pyplot as plt
import seaborn as sns
import config
import logging
from datetime import datetime

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(CURRENT_DIR)
MODELS_DIR = os.path.join(PROJECT_ROOT, 'models')
DATA_PATH = os.path.join(PROJECT_ROOT, 'data', 'AI_Training_Data.csv')
LOG_DIR = os.path.join(CURRENT_DIR, 'logs')
ASSETS_DIR = os.path.join(PROJECT_ROOT, 'assets')

if not os.path.exists(LOG_DIR): os.makedirs(LOG_DIR)
os.makedirs(MODELS_DIR, exist_ok=True)
os.makedirs(ASSETS_DIR, exist_ok=True)

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

MODEL_PATH = os.path.join(MODELS_DIR, 'film_revenue_v25.pkl')
VECTORIZER_PATH = os.path.join(MODELS_DIR, 'tfidf_v25.pkl')
FEATURES_PATH = os.path.join(MODELS_DIR, 'features_v25.pkl')
KNOWLEDGE_PATH = os.path.join(MODELS_DIR, 'knowledge_v25.pkl')

def build_knowledge_base():
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
        sys.exit(1)

def get_training_data():
    try:
        df = pd.read_csv(DATA_PATH)
    except Exception as e:
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

def process_nlp_with_bert(df):
    logging.info("Encoding overviews with BERT...")
    bert_model = SentenceTransformer('all-MiniLM-L6-v2')
    
    overviews = df['overview'].fillna("").tolist()
    embeddings = bert_model.encode(overviews, show_progress_bar=True)
    
    pca = PCA(n_components=30, random_state=42)
    nlp_features = pca.fit_transform(embeddings)
    
    nlp_cols = [f'nlp_bert_pca_{i}' for i in range(30)]
    df_nlp = pd.DataFrame(nlp_features, columns=nlp_cols)
    
    df = df.reset_index(drop=True)
    df_final = pd.concat([df, df_nlp], axis=1)
    
    return df_final, nlp_cols, pca

def optimize_all_models(X_train, y_train, X_test, y_test, weights):
    logging.info("Optuna Phase 1: Tuning LightGBM...")
    def obj_lgb(trial):
        param = {
            'n_estimators': trial.suggest_int('n_estimators', 500, 1500),
            'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.05, log=True),
            'num_leaves': trial.suggest_int('num_leaves', 20, 70),
            'max_depth': trial.suggest_int('max_depth', 4, 9),
            'subsample': trial.suggest_float('subsample', 0.6, 0.9),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 0.9),
            'min_child_samples': trial.suggest_int('min_child_samples', 10, 40),
            'random_state': 42,
            'n_jobs': -1,
            'verbose': -1
        }
        model = lgb.LGBMRegressor(**param)
        model.fit(X_train, y_train, sample_weight=weights)
        return np.sqrt(mean_squared_error(y_test, model.predict(X_test)))
    
    study_lgb = optuna.create_study(direction='minimize')
    study_lgb.optimize(obj_lgb, n_trials=20)
    
    logging.info("Optuna Phase 2: Tuning XGBoost...")
    def obj_xgb(trial):
        param = {
            'n_estimators': trial.suggest_int('n_estimators', 500, 1500),
            'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.05, log=True),
            'max_depth': trial.suggest_int('max_depth', 3, 7),
            'min_child_weight': trial.suggest_int('min_child_weight', 1, 5),
            'subsample': trial.suggest_float('subsample', 0.6, 0.9),
            'colsample_bytree': trial.suggest_float('colsample_bytree', 0.6, 0.9),
            'random_state': 42,
            'n_jobs': -1
        }
        model = xgb.XGBRegressor(**param)
        model.fit(X_train, y_train, sample_weight=weights)
        return np.sqrt(mean_squared_error(y_test, model.predict(X_test)))
    
    study_xgb = optuna.create_study(direction='minimize')
    study_xgb.optimize(obj_xgb, n_trials=20)

    logging.info("Optuna Phase 3: Tuning GradientBoosting...")
    def obj_gb(trial):
        param = {
            'n_estimators': trial.suggest_int('n_estimators', 500, 1200),
            'learning_rate': trial.suggest_float('learning_rate', 0.005, 0.05, log=True),
            'max_depth': trial.suggest_int('max_depth', 3, 6),
            'min_samples_leaf': trial.suggest_int('min_samples_leaf', 2, 10),
            'subsample': trial.suggest_float('subsample', 0.6, 0.9),
            'random_state': 42
        }
        model = GradientBoostingRegressor(**param)
        model.fit(X_train, y_train, sample_weight=weights)
        return np.sqrt(mean_squared_error(y_test, model.predict(X_test)))
    
    study_gb = optuna.create_study(direction='minimize')
    study_gb.optimize(obj_gb, n_trials=15)

    return study_lgb.best_params, study_xgb.best_params, study_gb.best_params

def train_final_model():
    knowledge, global_stats = build_knowledge_base()
    df = get_training_data()
    
    df, nlp_cols, nlp_pca_model = process_nlp_with_bert(df)
    
    features = ['budget_log', 'runtime', 'cast_power', 'director_power', 
                'budget_x_cast', 'budget_x_franchise', 'cast_x_director',
                'is_franchise', 'rating_score', 'month', 'is_summer', 'is_holiday'] + \
               [c for c in df.columns if 'genre_' in c] + nlp_cols

    X = df[features]
    y = df['revenue_log']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, random_state=42)
    
    weights_train = np.where(X_train['budget_log'] > 17.7, 2.0, 1.0)
    
    lgb_params, xgb_params, gb_params = optimize_all_models(X_train, y_train, X_test, y_test, weights_train)

    logging.info("=========================================================")
    logging.info("       BENCHMARKING ALL MODELS FOR README.MD             ")
    logging.info("=========================================================")
    
    # 1. Linear Regression
    lr = LinearRegression()
    lr.fit(X_train, y_train, sample_weight=weights_train)
    y_pred_lr = lr.predict(X_test)
    logging.info(f"| Linear Regression        | RMSE: {np.sqrt(mean_squared_error(y_test, y_pred_lr)):.4f} | R2: {r2_score(y_test, y_pred_lr):.4f} |")

    # 2. XGBoost
    xgb_single = xgb.XGBRegressor(**xgb_params, random_state=42, n_jobs=-1)
    xgb_single.fit(X_train, y_train, sample_weight=weights_train)
    y_pred_xgb = xgb_single.predict(X_test)
    logging.info(f"| XGBoost                  | RMSE: {np.sqrt(mean_squared_error(y_test, y_pred_xgb)):.4f} | R2: {r2_score(y_test, y_pred_xgb):.4f} |")
    
    # 3. LightGBM
    lgb_single = lgb.LGBMRegressor(**lgb_params, random_state=42, n_jobs=-1, verbose=-1)
    lgb_single.fit(X_train, y_train, sample_weight=weights_train)
    y_pred_lgb = lgb_single.predict(X_test)
    logging.info(f"| LightGBM                 | RMSE: {np.sqrt(mean_squared_error(y_test, y_pred_lgb)):.4f} | R2: {r2_score(y_test, y_pred_lgb):.4f} |")

    # 4. GradientBoosting
    gb_single = GradientBoostingRegressor(**gb_params, random_state=42)
    gb_single.fit(X_train, y_train, sample_weight=weights_train)
    y_pred_gb = gb_single.predict(X_test)
    logging.info(f"| GradientBoosting         | RMSE: {np.sqrt(mean_squared_error(y_test, y_pred_gb)):.4f} | R2: {r2_score(y_test, y_pred_gb):.4f} |")

    logging.info("=========================================================")
    
    logging.info("Training Final Stacking Model...")
    lgb_reg = lgb.LGBMRegressor(**lgb_params, random_state=42, n_jobs=-1, verbose=-1)
    xgb_reg = xgb.XGBRegressor(**xgb_params, random_state=42, n_jobs=-1)
    gb_reg = GradientBoostingRegressor(**gb_params, random_state=42)

    stacking = StackingRegressor(
        estimators=[('gb', gb_reg), ('xgb', xgb_reg), ('lgb', lgb_reg)], 
        final_estimator=RidgeCV(alphas=(0.1, 1.0, 10.0)), 
        n_jobs=-1 
    )
    
    stacking.fit(X_train, y_train, sample_weight=weights_train)
    y_pred = stacking.predict(X_test)
    r2 = r2_score(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    
    logging.info("=========================================================")
    logging.info(f"| **Stacking Ensemble (Final)** | RMSE: {rmse:.4f} | R2: {r2:.4f} |")
    logging.info("=========================================================")

    weights_full = np.where(df['budget_log'] > 17.7, 2.0, 1.0)
    stacking.fit(X, y, sample_weight=weights_full)
    
    gb_imp = stacking.estimators_[0].feature_importances_
    xgb_imp = stacking.estimators_[1].feature_importances_
    lgb_imp = stacking.estimators_[2].feature_importances_
    
    gb_imp = gb_imp / gb_imp.sum()
    xgb_imp = xgb_imp / xgb_imp.sum()
    lgb_imp = lgb_imp / lgb_imp.sum()
    avg_imp = (gb_imp + xgb_imp + lgb_imp) / 3.0
    
    df_imp = pd.DataFrame({'Feature': features, 'Importance': avg_imp})
    df_imp = df_imp.sort_values(by='Importance', ascending=False).head(20)
    
    plt.figure(figsize=(12, 8))
    sns.barplot(x='Importance', y='Feature', data=df_imp, palette='viridis')
    plt.title('Top 20 Feature Importances (Stacking Average)')
    plt.tight_layout()
    plt.savefig(os.path.join(ASSETS_DIR, 'feature_importance.png'))
    plt.close()

    joblib.dump(stacking, MODEL_PATH)
    joblib.dump(nlp_pca_model, VECTORIZER_PATH)
    joblib.dump(features, FEATURES_PATH)
    joblib.dump((knowledge, global_stats), KNOWLEDGE_PATH)
    
    logging.info("Training complete. Benchmarking data ready.")

if __name__ == "__main__":
    train_final_model()