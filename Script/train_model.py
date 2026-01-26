import pandas as pd
import numpy as np
import joblib
import urllib
from sqlalchemy import create_engine
import sys
import difflib
import warnings
import time
import matplotlib.pyplot as plt  # Thư viện vẽ biểu đồ
import seaborn as sns            # Thư viện vẽ đẹp

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.decomposition import TruncatedSVD
from sklearn.pipeline import make_pipeline
from sklearn.ensemble import StackingRegressor, GradientBoostingRegressor, RandomForestRegressor
from sklearn.linear_model import LinearRegression, Ridge
import xgboost as xgb
import lightgbm as lgb
import config

warnings.filterwarnings("ignore")

# --- CẤU HÌNH ---
DB_CONN_STR = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config.SERVER_NAME};DATABASE=Movie_DB;Trusted_Connection=yes;"
MODEL_PATH = 'film_revenue_v25.pkl'
VECTORIZER_PATH = 'tfidf_v25.pkl'
FEATURES_PATH = 'features_v25.pkl'
KNOWLEDGE_PATH = 'knowledge_v25.pkl'


def build_knowledge_base():
    print("💾 [1/7] Đang tải Kho tri thức từ SQL Server...")
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
    except Exception as e: sys.exit(f"❌ SQL Error: {e}")


def adjust_for_inflation(row):
    rate = 0.028 
    years_diff = 2025 - row.get('year', 2015)
    if years_diff < 0: years_diff = 0
    multiplier = (1 + rate) ** years_diff
    
    row['revenue_adj'] = row['revenue'] * multiplier
    row['budget_adj'] = row['budget'] * multiplier
    return row

def get_training_data():
    print("📥 [2/7] Xử lý dữ liệu & Tính toán Lạm phát...")
    try: df = pd.read_csv('AI_Training_Data.csv')
    except: sys.exit("❌ Không tìm thấy file 'AI_Training_Data.csv'. Hãy chạy feature_engineering.py trước.")
    
    if 'year' not in df.columns: df['year'] = np.random.randint(2000, 2024, size=len(df))
    if 'month' not in df.columns: df['month'] = np.random.randint(1, 13, size=len(df))
    
    df['is_summer'] = df['month'].apply(lambda x: 1 if x in [5,6,7,8] else 0)
    df['is_holiday'] = df['month'].apply(lambda x: 1 if x in [11,12] else 0)
    
    try:
        engine = create_engine(f"mssql+pyodbc:///?odbc_connect={urllib.parse.quote_plus(DB_CONN_STR)}")
        df_over = pd.read_sql("SELECT movie_id, overview FROM Dim_Movie_Rich_Info", engine)
        df = df.merge(df_over, on='movie_id', how='left').fillna({'overview': ''})
    except: df['overview'] = ""

    df = df[df['revenue'] > 1000000].copy() # Chỉ học phim thương mại (>1 Triệu $)
    df = df.apply(adjust_for_inflation, axis=1)
    
    df['revenue_log'] = np.log1p(df['revenue_adj'])
    df['budget_log'] = np.log1p(df['budget_adj'])
    df['cast_power'] = np.log1p(df['cast_power'])
    df['director_power'] = np.log1p(df['director_power'])
    
    df['budget_x_franchise'] = df['budget_log'] * df['is_franchise'] 
    df['budget_x_cast'] = df['budget_log'] * df['cast_power']
    df['cast_x_director'] = df['cast_power'] * df['director_power']
    
    return df


def train_final_model():
    print("="*60); print("🚀 HOLLYWOOD AI v25: TRAINING & BENCHMARKING"); print("="*60)
    
    knowledge, global_stats = build_knowledge_base()
    df = get_training_data()
    
    print("📚 [3/7] NLP Processing...")
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

    print("✂️ [4/7] Chia tập dữ liệu (85% Train - 15% Test)...")
    X = df[features]
    y = df['revenue_log']
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.15, random_state=42)
    
    # ==========================================================================
    # PHẦN MỚI: CHẠY BENCHMARKING (SO SÁNH 4 MODELS)
    # ==========================================================================
    print("\n⚡ [5/7] BENCHMARKING: Đang huấn luyện so sánh các mô hình...")
    
    results = {}
    
    # 1. Linear Regression (Baseline)
    print("   > Training Linear Regression...")
    lr = LinearRegression()
    lr.fit(X_train, y_train)
    y_pred_lr = lr.predict(X_test)
    results['Linear Regression'] = {
        'RMSE': np.sqrt(mean_squared_error(y_test, y_pred_lr)),
        'MAE': mean_absolute_error(y_test, y_pred_lr),
        'R2': r2_score(y_test, y_pred_lr)
    }

    # 2. Random Forest
    print("   > Training Random Forest...")
    rf = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
    rf.fit(X_train, y_train)
    y_pred_rf = rf.predict(X_test)
    results['Random Forest'] = {
        'RMSE': np.sqrt(mean_squared_error(y_test, y_pred_rf)),
        'MAE': mean_absolute_error(y_test, y_pred_rf),
        'R2': r2_score(y_test, y_pred_rf)
    }

    # 3. XGBoost Single
    print("   > Training XGBoost (Single)...")
    xgb_single = xgb.XGBRegressor(n_estimators=1500, learning_rate=0.02, max_depth=6, n_jobs=-1, random_state=42)
    xgb_single.fit(X_train, y_train)
    y_pred_xgb = xgb_single.predict(X_test)
    results['XGBoost (Single)'] = {
        'RMSE': np.sqrt(mean_squared_error(y_test, y_pred_xgb)),
        'MAE': mean_absolute_error(y_test, y_pred_xgb),
        'R2': r2_score(y_test, y_pred_xgb)
    }

    # 4. Proposed Stacking (Boss)
    print("   > Training PROPOSED STACKING...")
    gb_reg = GradientBoostingRegressor(n_estimators=1500, learning_rate=0.02, max_depth=5, random_state=42)
    xgb_reg = xgb.XGBRegressor(n_estimators=1500, learning_rate=0.02, max_depth=6, n_jobs=-1, random_state=42)
    lgb_reg = lgb.LGBMRegressor(n_estimators=1500, learning_rate=0.02, num_leaves=35, verbose=-1, n_jobs=-1, random_state=42)

    stacking = StackingRegressor(
        estimators=[('gb', gb_reg), ('xgb', xgb_reg), ('lgb', lgb_reg)], 
        final_estimator=LinearRegression(), 
        n_jobs=-1 
    )
    
    stacking.fit(X_train, y_train)
    y_pred_stack = stacking.predict(X_test)
    results['Proposed Stacking'] = {
        'RMSE': np.sqrt(mean_squared_error(y_test, y_pred_stack)),
        'MAE': mean_absolute_error(y_test, y_pred_stack),
        'R2': r2_score(y_test, y_pred_stack)
    }

    # ==========================================================================
    # IN BẢNG KẾT QUẢ CHO BÀI BÁO
    # ==========================================================================
    print("\n" + "="*80)
    print("🧪 TABLE I: MODEL PERFORMANCE COMPARISON (LOG SCALE) - COPY VÀO BÁO CÁO")
    print("="*80)
    print(f"{'Model Architecture':<25} | {'RMSE':<10} | {'MAE':<10} | {'R² Score':<10}")
    print("-" * 80)
    
    for name, metrics in results.items():
        print(f"{name:<25} | {metrics['RMSE']:.4f}     | {metrics['MAE']:.4f}     | {metrics['R2']:.4f}")
    print("="*80 + "\n")

    # ==========================================================================
    # VẼ BIỂU ĐỒ FEATURE IMPORTANCE
    # ==========================================================================
    try:
        # Lấy model XGBoost ra (vị trí index 1 trong estimators)
        xgb_extracted = stacking.estimators_[1] 
        importance = xgb_extracted.feature_importances_
        fi_df = pd.DataFrame({'Feature': X.columns, 'Importance': importance})
        fi_df = fi_df.sort_values(by='Importance', ascending=False).head(15)

        plt.figure(figsize=(10, 6))
        sns.barplot(x='Importance', y='Feature', data=fi_df, palette='viridis')
        plt.title('Feature Importance (Top 15 Predictors)', fontsize=14)
        plt.xlabel('Importance Score')
        plt.tight_layout()
        plt.savefig('feature_importance.png', dpi=300)
        print("✅ [INFO] Đã lưu biểu đồ: 'feature_importance.png'")
    except Exception as e:
        print(f"⚠️ Không thể vẽ biểu đồ: {e}")

    # ==========================================================================
    # RETRAIN FULL & SAVE
    # ==========================================================================
    print("🔄 [6/7] Huấn luyện lại Stacking trên TOÀN BỘ DỮ LIỆU...")
    stacking.fit(X, y)
    
    print("💾 [7/7] Đang lưu mô hình...")
    joblib.dump(stacking, MODEL_PATH)
    joblib.dump(nlp_pipe, VECTORIZER_PATH)
    joblib.dump(features, FEATURES_PATH)
    joblib.dump((knowledge, global_stats), KNOWLEDGE_PATH)
    
    # --- KHỞI ĐỘNG TOOL ---
    run_prediction_tool(stacking, nlp_pipe, features, knowledge, global_stats)


def get_power_score(name, role_dict, global_stats):
    if not name or len(name.strip()) < 2: return np.log1p(global_stats['avg_revenue']), 0
    
    matches = difflib.get_close_matches(name, role_dict.keys(), n=1, cutoff=0.6)
    
    if matches:
        real_name = matches[0]
        raw_val = role_dict[real_name]['avg_revenue']
        count = role_dict[real_name].get('movie_count', 1)
        print(f"      Found: {real_name} (TB: ${raw_val:,.0f} / {count} movies)")
        return np.log1p(raw_val), raw_val
    else:
        print(f"      Cannot Find '{name}' -> Using industry's average")
        return np.log1p(global_stats['avg_revenue']), 0

def analyze_risk_and_safety(budget, raw_pred, dir_raw, cast_raw, is_franchise, overview):
    """
    Đây là bộ não của 'Quỹ Đầu Tư'. Nó dùng Business Logic để chặn các dự án ảo tưởng.
    """
    risk_score = 0
    warnings = []
    
    # 1. BẪY NGÂN SÁCH (High Budget Trap)
    if budget > 200_000_000:
        if dir_raw < 150_000_000: # Đạo diễn chưa có lịch sử doanh thu cao
            risk_score += 0.25 # Trừ 25% doanh thu
            warnings.append("⚠️ Đạo diễn chưa có kinh nghiệm với dự án bom tấn >$200M.")
        if cast_raw < 150_000_000: # Diễn viên chưa đủ nhiệt
            risk_score += 0.20 # Trừ 20%
            warnings.append("⚠️ Diễn viên chính chưa phải là ngôi sao bảo chứng phòng vé.")

    # 2. BẪY THƯƠNG HIỆU (Franchise Fatigue)
    if is_franchise:
        hype_keywords = ['epic', 'finale', 'conclusion', 'return', 'war', 'battle', 'saga']
        if not any(k in overview.lower() for k in hype_keywords):
            risk_score += 0.15
            warnings.append("⚠️ Phim thương hiệu (Sequel) nhưng cốt truyện thiếu yếu tố đột phá.")
            
    # 3. TÍNH TOÁN CON SỐ AN TOÀN
    safety_margin = 0.15 # Luôn trừ hao 15% cho sai số mô hình
    
    final_pred = raw_pred * (1 - safety_margin - risk_score)
    
    # Tính Worst Case (Trường hợp xấu nhất: Thị trường sập)
    worst_case = final_pred * 0.7 
    
    return final_pred, worst_case, warnings, risk_score

# ==============================================================================
# 5. GIAO DIỆN NGƯỜI DÙNG (USER INTERFACE)
# ==============================================================================
def run_prediction_tool(model, nlp_pipe, features, knowledge, global_stats):
    valid_genres = [c.replace('genre_', '') for c in features if 'genre_' in c]
    
    while True:
        print("\n" + "="*70)
        print("🎬 HỆ THỐNG THẨM ĐỊNH ĐẦU TƯ ĐIỆN ẢNH (ENTERPRISE EDITION)")
        print("="*70)
        try:
            # 1. NHẬP LIỆU
            movie_name = input("1. 🎬 Tên Dự Án: ")
            if movie_name.lower() == 'exit': break
            
            overview = input("2. 📝 Cốt truyện (English): ")
            try: budget = float(input("3. 💰 Ngân sách Đầu tư ($): "))
            except: budget = 100000000
            
            director = input("4. 🎬 Đạo diễn: ")
            dir_power, dir_raw = get_power_score(director, knowledge['Director'], global_stats)
            
            actor = input("5. ⭐ Diễn viên chính: ")
            cast_power, cast_raw = get_power_score(actor, knowledge['Actor'], global_stats)
            
            try: month = int(input("6. 🗓️ Tháng phát hành (1-12): "))
            except: month = 6
            
            fran_in = input("7. 🔗 Franchise/Sequel? (y/n): ")
            is_franchise = 1 if fran_in.lower().startswith('y') else 0
            
            print(f"   (Gợi ý: {', '.join(valid_genres[:4])}...)")
            genre_in = input("8. 🎭 Thể loại: ")
            
            # 2. XỬ LÝ DỮ LIỆU INPUT
            budget_log = np.log1p(budget)
            
            idf = pd.DataFrame(0.0, index=[0], columns=features)
            idf['budget_log'] = budget_log
            idf['runtime'] = 120 # Mặc định
            idf['cast_power'] = cast_power
            idf['director_power'] = dir_power
            idf['is_franchise'] = is_franchise
            idf['rating_score'] = 2 # PG-13
            idf['month'] = month
            idf['is_summer'] = 1 if month in [5,6,7,8] else 0
            idf['is_holiday'] = 1 if month in [11,12] else 0
            
            # Tương tác
            idf['budget_x_cast'] = budget_log * cast_power
            idf['budget_x_franchise'] = budget_log * is_franchise
            idf['cast_x_director'] = cast_power * dir_power
            
            for col in features:
                if 'genre_' in col and genre_in.lower() in col.lower(): idf[col] = 1.0
            
            vec = nlp_pipe.transform([overview])
            for i in range(30): 
                if f'nlp_{i}' in idf.columns: idf[f'nlp_{i}'] = vec[0, i]
            
            # 3. DỰ BÁO & PHÂN TÍCH RỦI RO
            print("\n⏳ AI ĐANG CHẠY MÔ HÌNH & KIỂM TRA RỦI RO...")
            pred_log = model.predict(idf)[0]
            raw_pred = np.expm1(pred_log)
            
            # Gọi bộ lọc rủi ro
            safe_pred, worst_case, warnings, risk_score = analyze_risk_and_safety(
                budget, raw_pred, dir_raw, cast_raw, is_franchise, overview
            )
            
            roi = (safe_pred - budget) / budget
            
            # 4. XUẤT BÁO CÁO
            print("\n" + "╔" + "═"*60 + "╗")
            print(f"║ 📑 BÁO CÁO THẨM ĐỊNH: {movie_name.upper():<36} ║")
            print("╠" + "═"*60 + "╣")
            print(f"║ 💰 NGÂN SÁCH:          ${budget:,.0f}                     ║")
            print(f"║ 🤖 AI DỰ BÁO (Gốc):     ${raw_pred:,.0f}                     ║")
            print("╟" + "─"*60 + "╢")
            print(f"║ 🛡️ DỰ BÁO AN TOÀN:     ${safe_pred:,.0f} (Đã trừ rủi ro)     ║")
            print(f"║ 📉 TRƯỜNG HỢP XẤU NHẤT: ${worst_case:,.0f}                     ║")
            print("╟" + "─"*60 + "╢")
            print(f"║ 📈 ROI KỲ VỌNG:        {roi:.2%}                            ║")
            print("╚" + "═"*60 + "╝")
            
            if warnings:
                print("\n⚠️ CÁC YẾU TỐ RỦI RO ĐƯỢC PHÁT HIỆN:")
                for w in warnings: print(f"   - {w}")
            else:
                print("\n✅ DỰ ÁN CÓ CHỈ SỐ SỨC KHỎE TỐT.")
                
            print("\n🏁 KHUYẾN NGHỊ ĐẦU TƯ:")
            if roi > 1.5: print("   🚀 SIÊU BOM TẤN - NÊN ĐẦU TƯ MẠNH")
            elif roi > 0.3: print("   ✅ AN TOÀN - NÊN ĐẦU TƯ")
            elif roi > 0: print("   ⚠️ RỦI RO TRUNG BÌNH - CÂN NHẮC KỸ")
            else: print("   🛑 NGUY HIỂM - KHÔNG NÊN ĐẦU TƯ")
            
        except Exception as e: print(f"❌ Error: {e}")

if __name__ == "__main__":
    train_final_model()
    