import streamlit as st
import pandas as pd
import numpy as np
import joblib
import os
import sys
from sentence_transformers import SentenceTransformer

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
        
        model_path = os.path.join(current_dir, 'models', 'film_revenue_v25.pkl')
        pca_path = os.path.join(current_dir, 'models', 'tfidf_v25.pkl')
        features_path = os.path.join(current_dir, 'models', 'features_v25.pkl')
        knowledge_path = os.path.join(current_dir, 'models', 'knowledge_v25.pkl')

        model = joblib.load(model_path)
        pca_model = joblib.load(pca_path)
        features = joblib.load(features_path)
        
        knowledge_data = joblib.load(knowledge_path)
        if isinstance(knowledge_data, (tuple, list)) and len(knowledge_data) == 2:
            knowledge, global_stats = knowledge_data
        else:
            knowledge = knowledge_data
            global_stats = {'avg_revenue': 50000000}

        bert_model = SentenceTransformer('all-MiniLM-L6-v2')

        return model, pca_model, bert_model, features, knowledge, global_stats
    except Exception as e:
        st.error(f"Error loading resources: {e}")
        return None, None, None, None, None, None

model, pca_model, bert_model, features, knowledge, global_stats = load_resources()

if model is None: 
    st.warning("Waiting for model to load...")
    st.stop()

available_genres = [f.replace('genre_', '') for f in features if f.startswith('genre_')]

tab_bi, tab_ai = st.tabs(["Dashboard", "AI Prediction Tool"])

with tab_bi:
    base_url = "https://app.powerbi.com/view?r=eyJrIjoiN2Q0ZjcxY2EtNmRlNy00Y2VjLTg4MGQtZDE5YjRlYmYyY2U5IiwidCI6IjVlOGIzMjY5LTc2Y2EtNDU3Yy04NDdmLTQ0NGUzZGI5ODZhNyIsImMiOjl9"
    final_link = f"{base_url}&pageName=ReportSection1"
    st.markdown(f'<iframe title="Film Analysis" src="{final_link}" allowFullScreen="true"></iframe>', unsafe_allow_html=True)

with tab_ai:
    with st.container():
        st.markdown('<div style="padding: 20px;">', unsafe_allow_html=True)
        st.markdown("### Movie Project Analysis")
        
        col_input, col_result = st.columns([1, 1.5])
        with col_input:
            movie_name = st.text_input("Movie Title", "", placeholder="Enter project name...")
            overview = st.text_area("Overview (English)", "", placeholder="Enter plot summary...")
            
            c1, c2 = st.columns(2)
            with c1: budget = st.number_input("Budget ($)", min_value=0, value=0, step=1000000)
            with c2: runtime = st.number_input("Runtime (Minutes)", min_value=60, max_value=240, value=110)
            
            c3, c4 = st.columns(2)
            with c3: director = st.text_input("Director", "", placeholder="e.g. Christopher Nolan")
            with c4: actor = st.text_input("Lead Actor", "", placeholder="e.g. Robert Downey Jr.")
            
            c5, c6 = st.columns(2)
            with c5: month = st.slider("Release Month", 1, 12, 6)
            with c6: rating = st.selectbox("MPAA Rating", ['G', 'PG', 'PG-13', 'R', 'NC-17', 'NR'], index=2)
            
            is_fran = st.checkbox("Is Franchise?", value=False)
            selected_genres = st.multiselect("Genres", available_genres, default=['Action'] if 'Action' in available_genres else [])
            
            btn = st.button("Analyze Project", type="primary", use_container_width=True)

        with col_result:
            if btn:
                if not movie_name or len(overview) < 50 or not director or not actor or budget == 0:
                    st.warning("Please fill in all fields (Overview > 50 chars) to proceed.")
                else:
                    director_dict = knowledge.get('Director', {})
                    actor_dict = knowledge.get('Actor', {})

                    dir_power, dir_raw, _ = get_power_score(director, director_dict, global_stats)
                    cast_power, cast_raw, _ = get_power_score(actor, actor_dict, global_stats)
                    
                    rating_map = {'G': 0, 'PG': 1, 'PG-13': 2, 'R': 3, 'NC-17': 4, 'NR': 2}
                    
                    idf = pd.DataFrame(0.0, index=[0], columns=features)
                    
                    idf['budget_log'] = np.log1p(budget)
                    idf['runtime'] = runtime
                    idf['cast_power'] = cast_power
                    idf['director_power'] = dir_power
                    idf['is_franchise'] = 1 if is_fran else 0
                    idf['rating_score'] = rating_map.get(rating, 2)
                    idf['month'] = month
                    idf['is_summer'] = 1 if month in [5,6,7,8] else 0
                    idf['is_holiday'] = 1 if month in [11,12] else 0
                    idf['budget_x_cast'] = np.log1p(budget) * cast_power
                    idf['budget_x_franchise'] = np.log1p(budget) * (1 if is_fran else 0)
                    idf['cast_x_director'] = cast_power * dir_power
                    
                    for g in selected_genres:
                        if f"genre_{g}" in idf.columns:
                            idf[f"genre_{g}"] = 1.0
                    
                    bert_embedding = bert_model.encode([overview])
                    nlp_reduced = pca_model.transform(bert_embedding)
                    
                    for i in range(30):
                        pca_col = f'nlp_bert_pca_{i}'
                        if pca_col in idf.columns:
                            idf[pca_col] = nlp_reduced[0, i]
                    
                    df_predict = idf[features]
                    raw_pred = np.expm1(model.predict(df_predict)[0])
                    
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