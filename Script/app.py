import streamlit as st
import pandas as pd
import numpy as np
import joblib
import difflib
import os

# --- 1. CẤU HÌNH FULL MÀN HÌNH ---
st.set_page_config(page_title="Hollywood Dashboard", page_icon="🎬", layout="wide")

# --- 2. CSS FULL TRÀN VIỀN ---
st.markdown("""
    <style>
        .block-container {
            padding: 0rem !important;
            max-width: 100% !important;
        }
        header, footer {visibility: hidden;}
        
        /* Tab nằm gọn gàng trên nền tối */
        .stTabs {
            padding-left: 1rem;
            padding-top: 0.5rem;
            background-color: #0e1117; 
        }
        
        /* Iframe cao kịch kim */
        iframe {
            display: block;
            border: none;
            height: 94vh !important; /* Chiều cao tối đa */
            width: 100% !important;
        }
    </style>
""", unsafe_allow_html=True)

# --- 3. LOAD RESOURCES (ĐÃ SỬA CHUẨN) ---
@st.cache_resource
def load_resources():
    try:
        # Lấy đường dẫn tuyệt đối (Fix lỗi không tìm thấy file trên Cloud)
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        model_path = os.path.join(current_dir, 'film_revenue_v25.pkl')
        tfidf_path = os.path.join(current_dir, 'tfidf_v25.pkl')
        knowledge_path = os.path.join(current_dir, 'knowledge_v25.pkl')
        features_path = os.path.join(current_dir, 'features_v25.pkl')

        # Dùng joblib để load (Fix lỗi UnpicklingError)
        model = joblib.load(model_path)
        nlp_pipe = joblib.load(tfidf_path)
        features = joblib.load(features_path)
        
        # Load knowledge và tách ra thành 2 biến (Fix lỗi thiếu global_stats)
        knowledge_data = joblib.load(knowledge_path)
        if isinstance(knowledge_data, (tuple, list)) and len(knowledge_data) == 2:
            knowledge, global_stats = knowledge_data
        else:
            # Fallback nếu file lưu kiểu khác (tránh crash)
            knowledge = knowledge_data
            global_stats = {'avg_revenue': 50000000} # Giá trị mặc định an toàn

        return model, nlp_pipe, features, knowledge, global_stats
    except Exception as e:
        st.error(f"❌ Lỗi khi tải dữ liệu: {e}")
        return None, None, None, None, None

# Gọi hàm để lấy dữ liệu
model, nlp_pipe, features, knowledge, global_stats = load_resources()

# --- 4. LOGIC AI ---
def get_power_score(name, role_dict, global_stats):
    if not name or len(name.strip()) < 2: return np.log1p(global_stats['avg_revenue']), 0, None
    # Kiểm tra xem role_dict có phải là dict không để tránh lỗi
    if not isinstance(role_dict, dict): return np.log1p(global_stats['avg_revenue']), 0, None
    
    matches = difflib.get_close_matches(name, role_dict.keys(), n=1, cutoff=0.6)
    if matches:
        real_name = matches[0]
        raw_val = role_dict[real_name]['avg_revenue']
        return np.log1p(raw_val), raw_val, real_name
    return np.log1p(global_stats['avg_revenue']), 0, None

def analyze_risk(budget, raw_pred, dir_raw, cast_raw, is_franchise, overview):
    risk_score = 0; warnings = []
    if budget > 200_000_000:
        if dir_raw < 150_000_000: risk_score += 0.25; warnings.append("⚠️ Đạo diễn thiếu kinh nghiệm bom tấn.")
        if cast_raw < 150_000_000: risk_score += 0.20; warnings.append("⚠️ Diễn viên chưa đủ nhiệt.")
    if is_franchise and not any(k in overview.lower() for k in ['epic', 'finale', 'war']):
        risk_score += 0.15; warnings.append("⚠️ Sequel thiếu yếu tố đột phá.")
    final_pred = raw_pred * (1 - 0.15 - risk_score)
    return final_pred, final_pred * 0.7, warnings, risk_score

# --- 5. GIAO DIỆN CHÍNH ---

if model is None: 
    st.warning("Đang chờ tải Model...")
    st.stop()

tab_bi, tab_ai = st.tabs(["📊 DASHBOARD", "🤖 AI TOOL"])

# ==============================================================================
# TAB 1: DASHBOARD
# ==============================================================================
with tab_bi:
    base_url = "https://app.powerbi.com/view?r=eyJrIjoiN2Q0ZjcxY2EtNmRlNy00Y2VjLTg4MGQtZDE5YjRlYmYyY2U5IiwidCI6IjVlOGIzMjY5LTc2Y2EtNDU3Yy04NDdmLTQ0NGUzZGI5ODZhNyIsImMiOjl9"
    final_link = f"{base_url}&pageName=ReportSection1"

    st.markdown(f"""
        <iframe title="Film Analysis" 
        src="{final_link}" 
        allowFullScreen="true">
        </iframe>
    """, unsafe_allow_html=True)

# ==============================================================================
# TAB 2: AI TOOL
# ==============================================================================
with tab_ai:
    with st.container():
        st.markdown('<div style="padding: 20px;">', unsafe_allow_html=True)
        st.markdown("### 🤖 Thẩm định Dự án Phim")
        
        col_input, col_result = st.columns([1, 1.5])
        with col_input:
            movie_name = st.text_input("Tên phim", "", placeholder="Nhập tên dự án...")
            overview = st.text_area("Cốt truyện", "", placeholder="Nhập tóm tắt nội dung phim...")
            budget = st.number_input("Ngân sách ($)", min_value=0, value=0, step=1000000, help="Nhập số tiền đầu tư")
            
            c1, c2 = st.columns(2)
            with c1: director = st.text_input("Đạo diễn", "", placeholder="Ví dụ: Christopher Nolan")
            with c2: actor = st.text_input("Diễn viên", "", placeholder="Ví dụ: Robert Downey Jr.")
            
            is_fran = st.checkbox("Phim Franchise?", value=False)
            
            btn = st.button("🚀 CHẠY PHÂN TÍCH", type="primary", use_container_width=True)

        with col_result:
            if btn:
                if not movie_name or not overview or not director or not actor or budget == 0:
                    st.warning("⚠️ Vui lòng nhập đầy đủ thông tin trước khi phân tích!")
                else:
                    # Chạy Model
                    # Lấy knowledge dict từ biến đã load
                    # Kiểm tra an toàn cho dictionary
                    director_dict = knowledge.get('Director', {}) if isinstance(knowledge, dict) else {}
                    actor_dict = knowledge.get('Actor', {}) if isinstance(knowledge, dict) else {}

                    dir_power, dir_raw, _ = get_power_score(director, director_dict, global_stats)
                    cast_power, cast_raw, _ = get_power_score(actor, actor_dict, global_stats)
                    
                    idf = pd.DataFrame(0.0, index=[0], columns=features)
                    idf['budget_log'] = np.log1p(budget)
                    idf['cast_power'] = cast_power
                    idf['director_power'] = dir_power
                    idf['is_franchise'] = 1 if is_fran else 0
                    idf['budget_x_cast'] = np.log1p(budget) * cast_power
                    idf['budget_x_franchise'] = np.log1p(budget) * (1 if is_fran else 0)
                    idf['cast_x_director'] = cast_power * dir_power
                    
                    vec = nlp_pipe.transform([overview])
                    for i in range(30): idf[f'nlp_{i}'] = vec[0, i]
                    
                    raw_pred = np.expm1(model.predict(idf)[0])
                    safe, worst, warns, _ = analyze_risk(budget, raw_pred, dir_raw, cast_raw, is_fran, overview)
                    
                    st.success(f"Kết quả: {movie_name}")
                    m1, m2 = st.columns(2)
                    m1.metric("Doanh thu Mục tiêu", f"${safe:,.0f}")
                    m2.metric("ROI Kỳ vọng", f"{(safe-budget)/budget:.1%}")
                    if warns: [st.warning(w) for w in warns]
        st.markdown('</div>', unsafe_allow_html=True)