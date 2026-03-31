import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine
import os

# --- Page Configuration & UI Styling ---
st.set_page_config(
    page_title="Credit Risk Monitoring Pipeline",
    page_icon="🛡️",
    layout="wide"
)

# Custom CSS for Professional Dark Theme
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric {
        background-color: #161b22;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #30363d;
    }
    </style>
    """, unsafe_allow_html=True)

# --- Secure Database Connection ---
@st.cache_resource
def get_engine():
    # Fetching credentials from environment variables for security
    user = os.getenv("DW_POSTGRES_USER")
    password = os.getenv("DW_POSTGRES_PASSWORD")
    host = os.getenv("DW_POSTGRES_HOST")
    db = os.getenv("DW_POSTGRES_DB")
    
    if not all([user, password, host, db]):
        st.error("Missing Environment Variables!")
        st.stop()
    return create_engine(f"postgresql://{user}:{password}@{host}:5432/{db}")

engine = get_engine()

# --- Data Fetching (Using dbt pre-computed tables) ---
@st.cache_data(ttl=600)
def load_data():
    # Load aggregated summary and small sample to avoid RAM exhaustion
    df_summ = pd.read_sql("SELECT * FROM product.prd_risk_summary", engine)
    df_samp = pd.read_sql("SELECT * FROM product.prd_loan_sample", engine)
    return df_summ, df_samp

try:
    df_summary, df_sample = load_data()

    # --- Sidebar Filtering Logic ---
    st.sidebar.header("🔍 Analytics Filters")
    
    years = sorted(df_summary['data_year'].unique(), reverse=True)
    selected_year = st.sidebar.selectbox("Year", years)
    
    states = sorted(df_summary['state_name'].dropna().unique())
    selected_states = st.sidebar.multiselect("States", states, default=states[:5])

    # Efficient filtering on small DataFrames
    f_summ = df_summary[(df_summary['data_year'] == selected_year) & (df_summary['state_name'].isin(selected_states))]
    f_samp = df_sample[(df_sample['data_year'] == selected_year) & (df_sample['state_name'].isin(selected_states))]

    # --- Dashboard Header ---
    st.title(f"🚀 HMDA Credit Risk Dashboard")
    st.markdown("---")

    # --- Metrics Section ---
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Loans", f"{f_summ['loan_count'].sum():,}")
    with m2:
        st.metric("Portfolio Value", f"${f_summ['total_loan_amount'].sum()/1e6:.1f}M")
    with m3:
        # Calculate true weighted average interest rate
        avg_rate = f_summ['sum_interest_rate'].sum() / f_summ['loan_count'].sum() if not f_summ.empty else 0
        st.metric("Avg Market Rate", f"{avg_rate:.2f}%")
    with m4:
        # Calculate high risk concentration percentage
        hr_cnt = f_summ[f_summ['risk_segment'] == 'High']['loan_count'].sum()
        hr_pct = (hr_cnt / f_summ['loan_count'].sum() * 100) if not f_summ.empty else 0
        st.metric("High Risk %", f"{hr_pct:.1f}%")

    # --- Visualization Row 1 ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📊 Risk Hierarchy")
        # Hierarchical breakdown of risk and purpose
        fig_sun = px.sunburst(f_summ, path=['risk_segment', 'loan_purpose_name'], values='loan_count',
                             color='risk_segment', color_discrete_map={'High': '#ff4b4b', 'Medium': '#ffa421', 'Low': '#00d4ff'})
        st.plotly_chart(fig_sun, use_container_width=True)
    with c2:
        st.subheader("📍 Geography Heatmap")
        # Regional volume distribution
        map_df = f_summ.groupby('state_code')['loan_count'].sum().reset_index()
        fig_map = px.choropleth(map_df, locations='state_code', locationmode="USA-states", color='loan_count', scope="usa")
        st.plotly_chart(fig_map, use_container_width=True)

    # --- Visualization Row 2 ---
    c3, c4 = st.columns([1.2, 0.8])
    with c3:
        st.subheader("💰 Income vs Rate Spread (Sampled)")
        # Scatter plot using randomized sample for performance
        fig_scatter = px.scatter(f_samp, x='income', y='interest_rate_spread', color='risk_segment', size='loan_amount')
        st.plotly_chart(fig_scatter, use_container_width=True)
    with c4:
        st.subheader("🏦 Purpose Breakdown")
        fig_bar = px.bar(f_summ.groupby(['loan_purpose_name', 'risk_segment'])['loan_count'].sum().reset_index(),
                         x='loan_purpose_name', y='loan_count', color='risk_segment', barmode='group')
        st.plotly_chart(fig_bar, use_container_width=True)

except Exception as e:
    st.error(f"Pipeline Status: Pending. Error: {e}")