import streamlit as st
import pandas as pd
import numpy as np
import joblib
import os
import sys


ROOT_DIR = os.path.dirname(os.path.abspath(__file__))


if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)


from src.utils import get_power_score, analyze_risk_and_safety

st.set_page_config(page_title="Hollywood Dashboard", page_icon="🎬", layout="wide")


st.markdown("""
    <style>
        .block-container { padding: 0rem !important; max-width: 100% !important; }
        header, footer {visibility: hidden;}
        .stTabs { padding-left: 1rem; padding-top: 0.5rem; background-color: #0e1117; }
        iframe { display: block; border: none; height: 94vh !important; width: 100% !important; }
        div[data-testid="stMetricValue"] { font-size: 24px; font-weight: bold; }
    </style>
""", unsafe_allow_html=True)

@st.cache_resource
def load_resources():
    try:
        current_dir = os.path.dirname(os.path.abspath(__file__))
        
        # Đường dẫn đã chuẩn hóa vào folder models
        model_path = os.path.join(current_dir, 'models', 'film_revenue_v25.pkl')
        tfidf_path = os.path.join(current_dir, 'models', 'tfidf_v25.pkl')
        knowledge_path = os.path.join(current_dir, 'models', 'knowledge_v25.pkl')
        features_path = os.path.join(current_dir, 'models', 'features_v25.pkl')

        model = joblib.load(model_path)
        nlp_pipe = joblib.load(tfidf_path)
        features = joblib.load(features_path)
        
        knowledge_data = joblib.load(knowledge_path)
        if isinstance(knowledge_data, (tuple, list)) and len(knowledge_data) == 2:
            knowledge, global_stats = knowledge_data
        else:
            knowledge = knowledge_data
            global_stats = {'avg_revenue': 50000000}

        return model, nlp_pipe, features, knowledge, global_stats
    except Exception as e:
        st.error(f"Error loading resources: {e}")
        return None, None, None, None, None

model, nlp_pipe, features, knowledge, global_stats = load_resources()

if model is None: 
    st.warning("Waiting for model to load...")
    st.stop()

tab_bi, tab_ai = st.tabs(["Dashboard", "AI Prediction Tool"])

# PowerBI
with tab_bi:
    base_url = "https://app.powerbi.com/view?r=eyJrIjoiN2Q0ZjcxY2EtNmRlNy00Y2VjLTg4MGQtZDE5YjRlYmYyY2U5IiwidCI6IjVlOGIzMjY5LTc2Y2EtNDU3Yy04NDdmLTQ0NGUzZGI5ODZhNyIsImMiOjl9"
    final_link = f"{base_url}&pageName=ReportSection1"
    st.markdown(f'<iframe title="Film Analysis" src="{final_link}" allowFullScreen="true"></iframe>', unsafe_allow_html=True)

# AI
with tab_ai:
    with st.container():
        st.markdown('<div style="padding: 20px;">', unsafe_allow_html=True)
        st.markdown("### Movie Project Analysis")
        
        col_input, col_result = st.columns([1, 1.5])
        with col_input:
            movie_name = st.text_input("Movie Title", "", placeholder="Enter project name...")
            overview = st.text_area("Overview (English)", "", placeholder="Enter plot summary...")
            budget = st.number_input("Budget ($)", min_value=0, value=0, step=1000000)
            
            c1, c2 = st.columns(2)
            with c1: director = st.text_input("Director", "", placeholder="e.g. Christopher Nolan")
            with c2: actor = st.text_input("Lead Actor", "", placeholder="e.g. Robert Downey Jr.")
            
            is_fran = st.checkbox("Is Franchise?", value=False)
            btn = st.button("Analyze Project", type="primary", use_container_width=True)

        with col_result:
            if btn:
                if not movie_name or not overview or not director or not actor or budget == 0:
                    st.warning("Please fill in all fields to proceed.")
                else:
                    # Lấy Dict
                    director_dict = knowledge.get('Director', {})
                    actor_dict = knowledge.get('Actor', {})

                    # Dùng hàm từ utils.py
                    dir_power, dir_raw, _ = get_power_score(director, director_dict, global_stats)
                    cast_power, cast_raw, _ = get_power_score(actor, actor_dict, global_stats)
                    
                    # Tạo DataFrame input
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
                    
                    # Model Predict thô
                    raw_pred = np.expm1(model.predict(idf)[0])
                    

                    safe, worst, warns = analyze_risk_and_safety(budget, raw_pred, dir_raw, cast_raw, is_fran, overview)
                    
                    roi = (safe - budget) / budget if budget > 0 else 0
                    
                    st.divider()
                    
                    verdict_color = "green"
                    verdict = "Positive market signal. Project shows good potential."
                    
                    if roi < 0:
                        st.error(f"Prediction: LOSS ({movie_name})")
                        verdict_color = "red"
                        verdict = "High commercial risk. Low probability of break-even."
                    elif roi < 0.3:
                        st.warning(f"Prediction: THIN MARGIN ({movie_name})")
                        verdict_color = "orange"
                        verdict = "Break-even possible but margins are tight."
                    else:
                        st.success(f"Prediction: PROFIT ({movie_name})")

                    c3, c4 = st.columns(2)
                    c3.metric("Predicted Revenue", f"${safe:,.0f}", delta_color="off")
                    c4.metric("Expected ROI", f"{roi:.1%}", delta=f"{roi:.1%}", delta_color="normal" if roi > 0 else "inverse")
                    
                    st.markdown(f"**AI Verdict:** :{verdict_color}[{verdict}]")

                    if warns:
                        with st.expander("Risk Factors Detected", expanded=True):
                            for w in warns: st.write(f"- {w}")

        st.markdown('</div>', unsafe_allow_html=True)