import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, text
import os

# --- Page Configuration & UI Styling ---
st.set_page_config(
    page_title="Credit Risk Monitoring Pipeline",
    page_icon="🛡️",
    layout="wide"
)

# Restore original clean colors
risk_colors = {'High': '#ff4b4b', 'Medium': '#ffa421', 'Low': '#00d4ff'}

# Removed px.defaults.template to seamlessly adapt to Light Mode

# Modern Light Mode CSS
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600&family=Outfit:wght@500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }
    h1, h2, h3, h4, .st-emotion-cache-10trblm {
        font-family: 'Outfit', sans-serif !important;
        color: #1f2937 !important;
    }
    
    /* Elegant soft blue-gray background */
    [data-testid="stAppViewContainer"] {
        background-color: #f0f4f8;
    }
    /* Solid white sidebar for contrast */
    [data-testid="stSidebar"] {
        background-color: #ffffff;
        border-right: 1px solid #e5e7eb;
    }
    
    /* White Card metrics with soft shadows */
    [data-testid="stMetric"] {
        background-color: #ffffff;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #e5e7eb;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.05); /* Slight elevation */
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    /* Bounce animation on hover */
    [data-testid="stMetric"]:hover {
        transform: translateY(-3px);
        box-shadow: 0 10px 15px -3px rgba(0, 0, 0, 0.1);
    }
    
    /* Subdued label color */
    [data-testid="stMetricLabel"] *, [data-testid="stMetricLabel"] {
        color: #6b7280 !important;
        font-weight: 500 !important;
        font-family: 'Inter', sans-serif;
    }
    /* Bold dark-gray metrics */
    [data-testid="stMetricValue"] {
        color: #111827 !important;
        font-family: 'Outfit', sans-serif;
        font-weight: 700 !important;
    }
    </style>
    """, unsafe_allow_html=True)

# --- Secure Database Connection ---
@st.cache_resource
def get_engine():
    user = os.getenv("DW_POSTGRES_USER")
    password = os.getenv("DW_POSTGRES_PASSWORD")
    host = os.getenv("DW_POSTGRES_HOST")
    db = os.getenv("DW_POSTGRES_DB")
    
    if not all([user, password, host, db]):
        st.error("Missing Environment Variables!")
        st.stop()
    return create_engine(f"postgresql://{user}:{password}@{host}:5432/{db}")

engine = get_engine()

# --- Optimization: Pushdown Filters Execution ---

@st.cache_data(ttl=86400)
def load_filters():
    """ Lightweight query to auto-fetch configuration (Year and State lists) """
    with engine.connect() as conn:
        years = pd.read_sql("SELECT DISTINCT data_year FROM credit_risk_prod.prd_risk_summary ORDER BY data_year DESC", conn)['data_year'].tolist()
        states = pd.read_sql("SELECT DISTINCT state_name FROM credit_risk_prod.prd_risk_summary WHERE state_name IS NOT NULL ORDER BY state_name", conn)['state_name'].tolist()
    return years, states

@st.cache_data(ttl=600)
def load_main_data(year, state_names):
    """ Push filter logic directly into SQL WHERE clause to avoid full table scans. """
    if not state_names:
        return pd.DataFrame(), pd.DataFrame()
    with engine.connect() as conn:
        # Format the SQL IN clause parameter array
        state_tuple = tuple(state_names) if len(state_names) > 1 else f"('{state_names[0]}')"
        
        query_summ = text(f"SELECT * FROM credit_risk_prod.prd_risk_summary WHERE data_year = :year AND state_name IN {state_tuple}")
        query_samp = text(f"SELECT * FROM credit_risk_prod.prd_loan_sample WHERE data_year = :year AND state_name IN {state_tuple}")
        
        df_summ = pd.read_sql(query_summ, conn, params={"year": year})
        df_samp = pd.read_sql(query_samp, conn, params={"year": year})
    return df_summ, df_samp

@st.cache_data(ttl=600)
def load_heatmap_data(year):
    """ Filter by year but retrieve full states to map the entire US. """
    with engine.connect() as conn:
        query_map = text("""
            SELECT state_code, SUM(loan_count) as loan_count 
            FROM credit_risk_prod.prd_risk_summary 
            WHERE data_year = :year 
            GROUP BY state_code
        """)
        df_map = pd.read_sql(query_map, conn, params={"year": year})
    return df_map

try:
    # 1. Pre-load filter choices
    nav_years, nav_states = load_filters()
    
    # 2. Sidebar Filters
    st.sidebar.header("🔍 Analytics Filters")
    selected_year = st.sidebar.selectbox("Year", nav_years)
    selected_states = st.sidebar.multiselect("States", nav_states, default=nav_states[:5] if nav_states else [])

    if not selected_states:
        st.warning("Please select at least one state.")
        st.stop()

    # 3. Load Data with Predicate Pushdown based on user selection
    df_summary, df_sample = load_main_data(selected_year, selected_states)
    df_map = load_heatmap_data(selected_year)  # Heatmap retrieves Full States for selected Year

    # --- Dashboard Header ---
    st.title("🚀 HMDA Credit Risk Dashboard")
    st.markdown("---")

    # --- Metrics Section ---
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Loans", f"{df_summary['loan_count'].sum():,}")
    with m2:
        st.metric("Portfolio Value", f"${df_summary['total_loan_amount'].sum()/1e6:.1f}M")
    with m3:
        total_loans = df_summary['loan_count'].sum()
        avg_rate = df_summary['sum_interest_rate'].sum() / total_loans if not df_summary.empty and total_loans > 0 else 0
        st.metric("Avg Market Rate", f"{avg_rate:.2f}%")
    with m4:
        total_loans = df_summary['loan_count'].sum()
        hr_cnt = df_summary[df_summary['risk_segment'] == 'High']['loan_count'].sum()
        hr_pct = (hr_cnt / total_loans * 100) if not df_summary.empty and total_loans > 0 else 0
        st.metric("High Risk %", f"{hr_pct:.1f}%")

    # --- Visualization Row 1 ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📊 Risk Hierarchy")
        if not df_summary.empty:
            fig_sun = px.sunburst(df_summary, path=['risk_segment', 'loan_purpose_name'], values='loan_count',
                                color='risk_segment', color_discrete_map=risk_colors)
            # Restore default interactive background in Light Theme
            st.plotly_chart(fig_sun, use_container_width=True)
        else:
            st.info("No data available")
    with c2:
        st.subheader("📍 Geography Heatmap")
        # Use specific full states data for the heatmap
        if not df_map.empty:
            # Revert to original Blues gradient
            fig_map = px.choropleth(df_map, locations='state_code', locationmode="USA-states", color='loan_count', scope="usa", color_continuous_scale="Blues")
            # Black bold text for better contrast on Blues
            fig_map.add_scattergeo(
                locations=df_map['state_code'],
                locationmode="USA-states",
                text=df_map['state_code'],
                mode="text",
                textfont=dict(color="black", size=10, weight="bold"),
                hoverinfo="skip"
            )
            # Optimize map padding to enlarge it
            fig_map.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.info("No data available for this year")

    # --- Visualization Row 2 ---
    c3, c4 = st.columns([1.2, 0.8])
    with c3:
        st.subheader("💰 Income vs Rate Spread (Sampled)")
        if not df_sample.empty:
            fig_scatter = px.scatter(df_sample, x='income', y='interest_rate_spread', color='risk_segment', size='loan_amount', color_discrete_map=risk_colors, opacity=0.5)
            # Hide grids to emphasize data points
            fig_scatter.update_xaxes(showgrid=False)
            fig_scatter.update_yaxes(showgrid=False)
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.info("No data available")
    with c4:
        st.subheader("🏦 Purpose Breakdown")
        if not df_summary.empty:
            grouped = df_summary.groupby(['loan_purpose_name', 'risk_segment'])['loan_count'].sum().reset_index()
            fig_bar = px.bar(grouped, x='loan_purpose_name', y='loan_count', color='risk_segment', barmode='group', color_discrete_map=risk_colors)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No data available")

except Exception as e:
    st.error(f"Pipeline Status: Pending. Error: {e}")