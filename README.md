
````markdown
#  Capital Efficiency in the Post-Digital Film Industry
### An End-to-End Machine Learning & BI Framework for ROI Prediction

![Python](https://img.shields.io/badge/Python-3.10+-3776AB?logo=python&logoColor=white)
![SQL Server](https://img.shields.io/badge/SQL_Server-Data_Warehouse-CC2927?logo=microsoft-sql-server&logoColor=white)
![Power BI](https://img.shields.io/badge/Power_BI-Business_Intelligence-F2C811?logo=power-bi&logoColor=black)
![Scikit-Learn](https://img.shields.io/badge/ML-Stacking_Regressor-F7931E?logo=scikit-learn&logoColor=white)

> **Executive Summary**  
> The film industry is facing a **Revenue Paradox**: while Gross Box Office reaches record highs, **net profit margins are collapsing** due to marketing and production cost inflation.  
>  
> This project moves beyond vanity metrics toward **Capital Efficiency**, using a **26-year dataset (2000–2025)** to identify *safe investment zones* and predict ROI with Machine Learning.

---

##  System Architecture — Data Pipeline

The system automates the journey from raw API data to investor-ready insights.

```mermaid
graph LR
    A[TMDB API] -->|Raw JSON| B(Python ETL)
    B -->|Cleaning & Business Logic| C[(SQL Server DW)]
    C -->|Star Schema| D{Power BI}
    C -->|Training Data| E[Stacking ML Model]
    E -->|Predictions| D
    D -->|Strategic Insights| F[Investor Report]

    style A fill:#f9f,stroke:#333,stroke-width:2px
    style C fill:#ccf,stroke:#333,stroke-width:2px
    style E fill:#ff9,stroke:#333,stroke-width:2px
````

**Key Design Choices**

* **Data Filters:** Commercial films only (Revenue > $1M, Budget > $1M)
* **Interactive Controls**

  * Marketing Cost Slider (investor-adjustable)
  * Exhibitor Revenue Share (default: 50%)

---

##  Phase 1 — Strategic Market Analysis (Business Intelligence)

### 1. Industry Health Check — *The Profit Squeeze*

**Dashboard:** Industry Financial Overview
![Industry Overview](assets/industry_overview_dashboard.jpg)

#### A. Revenue Paradox — Theatrical Break-even

* Revenue growth (2010–2019) was neutralized by parallel cost inflation
* Studios earn more but burn cash faster
* Theatrical release has shifted from **Profit Center** to **Marketing Center**

  * Streaming price anchor
  * IP validation layer

#### B. Marketing Efficiency Collapse

* Revenue per $1 Ad Spend

  * 2019: ~$7.5
  * Post-2020: ~$5.5
* Audience acquisition costs have structurally increased

#### C. Cost Structure Reality

* Exhibitors capture ~50% of GBO immediately
* Pandemic exposed margins thinner than 10%
* Studios bear production risk, cinemas secure guaranteed returns

#### D. The Money Burn — Waterfall Analysis

* Total industry revenue: $582B
* Net industry loss: **–$22.5B**
* Exhibitor share (–$291B) exceeds production budgets (–$209B)

---

### 2. Portfolio Strategy — The Barbell Investment Model

**Dashboard:** Risk, Reward & Seasonality
![Portfolio Strategy](assets/portfolio_strategy_dashboard.jpg)

#### A. Risk vs. Reward

* **Action / Adventure:** Massive upside (> $100M), severe downside (–$40M to –$50M)
* **Drama / Comedy:** Capital preservation, limited upside
* Cash spikes require controlled Action exposure

#### B. Capital Efficiency Curve

* ROI collapses beyond ~$60M budgets
* **Horror ($15–20M)** dominates ROI
* Bigger budgets ≠ better investments

#### C. Release Timing & Budget Matrix

* **October:** Horror peak profitability (Halloween Effect)
* **June–July:** Animation & Fantasy only
* **Death Valley:** $20M–$50M budgets, especially History / War in Q1

**Rule:**

> Go Big (> $100M) or Go Lean (< $10M). Avoid the middle.

---

### 3. Talent Economics — Unicorns vs. High Rollers

**Dashboard:** Director & Talent Performance
![Talent Economics](assets/talent_economics_dashboard.jpg)

#### A. Market Correction

* 2010–2015: High volume, high ROI (Golden Era)
* 2019–2021: ROI collapse
* 2022–Present: Fewer films, higher efficiency

#### B. Talent Strategy Matrix

* **Unicorns** (Cameron, Wan): High ROI, win rate > 90% → Greenlight immediately
* **High Rollers** (Nolan, Jackson): Massive revenue, volatile returns → Budget caps & profit-sharing
* **Safe Hands** (Favreau): Consistent, moderate upside → Portfolio stabilizers

#### C. Blockbuster Saturation

* Beyond ~18 top-tier director releases/year, marginal revenue declines
* Blockbuster capacity is finite

---

##  Phase 2 — Predictive Modeling (Machine Learning)

A **Stacking Ensemble Regressor** predicts box office revenue *before production begins*.

### Feature Engineering

* Leakage-safe rolling **Star Power**
* CPI-adjusted financials (2024 USD)
* Log-transformed revenue distributions

### Model Performance

| Model                         | RMSE (Log) | R²         |
| ----------------------------- | ---------- | ---------- |
| Linear Regression             | 1.1438     | 0.6082     |
| XGBoost                       | 1.1021     | 0.6362     |
| **Stacking Ensemble (Final)** | **1.1002** | **0.6374** |

![Feature Importance](assets/feature_importance.png)

> Budget size and franchise status dominate predictions — confirming the **Pay-to-Play** nature of modern cinema.

---

##  Final Recommendation — Action Plan

Based on **5,844 films**:

1. **Defensive (40%)**
   Horror / Thriller, Budget < $10M, Release in October
2. **Offensive (40%)**
   Franchise Animation / Action, Budget > $100M, Release June–July
3. **Risk Control**
   Exit direct production in History / War genres, shift to licensing
4. **Talent Policy**
   Prioritize directors with historical ROI > 0.1

---

##  How to Run

```bash
git clone https://github.com/YourRepo/Movie-Analysis-Pipeline.git
pip install -r requirements.txt
python src/feature_engineering.py
python src/train_model.py
streamlit run app.py
```

---

##  Author

**Lam Hai Duong**
*Analyzing the Art of Film with the Science of Data*


