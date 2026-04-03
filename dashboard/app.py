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

# --- Corporate Trust Theme Colors ---
risk_colors = {'High': '#E63946', 'Medium': '#B38D4F', 'Low': '#457B9D'}

# Corporate Edge CSS
st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&family=Playfair+Display:wght@600&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Roboto', sans-serif;
    }
    h1, h2, h3, h4, .st-emotion-cache-10trblm {
        font-family: 'Playfair Display', serif !important;
        color: #1D3557 !important;
    }
    
    /* Clean light gray background */
    [data-testid="stAppViewContainer"] {
        background-color: #F8F9FA;
    }
    /* Solid white sidebar with corporate navy border */
    [data-testid="stSidebar"] {
        background-color: #FFFFFF;
        border-right: 4px solid #1D3557;
    }
    
    /* Crisp White Metric Cards with subtle edge shadow */
    [data-testid="stMetric"] {
        background-color: #FFFFFF;
        padding: 20px;
        border-radius: 8px;
        border: 1px solid #E9ECEF;
        border-top: 4px solid #1D3557;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.05);
        transition: transform 0.2s ease, box-shadow 0.2s ease;
    }
    /* Polished hover effect */
    [data-testid="stMetric"]:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 12px rgba(29, 53, 87, 0.1);
        border-top: 4px solid #B38D4F;
    }
    
    /* Metric label styling */
    [data-testid="stMetricLabel"] *, [data-testid="stMetricLabel"] {
        color: #495057 !important;
        font-weight: 500 !important;
        text-transform: uppercase;
        font-size: 0.85rem;
        letter-spacing: 0.5px;
    }
    
    /* Metric value typography */
    [data-testid="stMetricValue"] {
        color: #1D3557 !important;
        font-weight: 700 !important;
        font-size: 2rem !important;
    }
    
    /* Streamlit tabs */
    .stTabs [data-baseweb="tab-list"] {
        gap: 24px;
    }
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        white-space: pre-wrap;
        background-color: transparent;
        border-radius: 4px 4px 0px 0px;
        gap: 1px;
        padding-top: 10px;
        padding-bottom: 10px;
        color: #495057;
        font-weight: 500;
    }
    .stTabs [aria-selected="true"] {
        background-color: #FFFFFF;
        color: #1D3557 !important;
        font-weight: 700 !important;
        border-top: 3px solid #B38D4F;
        border-left: 1px solid #E9ECEF;
        border-right: 1px solid #E9ECEF;
    }
    </style>
    """, unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────── #
# Backend Selection — controlled by DW_BACKEND in docker/.env                #
# Switch between PostgreSQL and Snowflake with a single env var change.       #
# ─────────────────────────────────────────────────────────────────────────── #
DW_BACKEND = os.getenv("DW_BACKEND", "postgresql")

# Schema where dbt mart tables live (mirrors dbt generate_schema_name macro):
#   PostgreSQL → credit_risk_prod  (profile schema 'credit_risk' + '_prod')
#   Snowflake  → PROD              (direct uppercase schema name)
MART_SCHEMA = "PROD" if DW_BACKEND == "snowflake" else "credit_risk_prod"


@st.cache_resource
def get_engine():
    """Return SQLAlchemy engine for the configured DW backend."""
    if DW_BACKEND == "snowflake":
        # Snowflake connection via snowflake-sqlalchemy
        from snowflake.sqlalchemy import URL as SnowflakeURL
        return create_engine(SnowflakeURL(
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            database=os.getenv("SNOWFLAKE_DATABASE", "CREDIT_RISK_DW"),
            schema=MART_SCHEMA,
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        ))
    else:
        # PostgreSQL connection (default)
        user     = os.getenv("DW_POSTGRES_USER")
        password = os.getenv("DW_POSTGRES_PASSWORD")
        host     = os.getenv("DW_POSTGRES_HOST")
        db       = os.getenv("DW_POSTGRES_DB")
        if not all([user, password, host, db]):
            st.error("Missing PostgreSQL environment variables!")
            st.stop()
        return create_engine(f"postgresql://{user}:{password}@{host}:5432/{db}")


engine = get_engine()


# --- Optimization: Pushdown Filters Execution ---

@st.cache_data(ttl=86400)
def load_filters():
    """Lightweight query to auto-fetch configuration (Year and State lists)."""
    with engine.connect() as conn:
        years  = pd.read_sql(
            f"SELECT DISTINCT data_year FROM {MART_SCHEMA}.prd_risk_summary ORDER BY data_year DESC",
            conn
        )['data_year'].tolist()
        states = pd.read_sql(
            f"SELECT DISTINCT state_name FROM {MART_SCHEMA}.prd_risk_summary WHERE state_name IS NOT NULL ORDER BY state_name",
            conn
        )['state_name'].tolist()
    return years, states


@st.cache_data(ttl=600)
def load_main_data(year, state_names):
    """Push filter logic directly into SQL WHERE clause to avoid full table scans."""
    if not state_names:
        return pd.DataFrame(), pd.DataFrame()
    with engine.connect() as conn:
        # Format the SQL IN clause parameter array
        state_tuple = tuple(state_names) if len(state_names) > 1 else f"('{state_names[0]}')"

        query_summ = text(
            f"SELECT * FROM {MART_SCHEMA}.prd_risk_summary WHERE data_year = :year AND state_name IN {state_tuple}"
        )
        query_samp = text(
            f"SELECT * FROM {MART_SCHEMA}.prd_loan_sample WHERE data_year = :year AND state_name IN {state_tuple}"
        )

        df_summ = pd.read_sql(query_summ, conn, params={"year": year})
        df_samp = pd.read_sql(query_samp, conn, params={"year": year})
    return df_summ, df_samp


@st.cache_data(ttl=600)
def load_heatmap_data(year):
    """Filter by year but retrieve full states to map the entire US."""
    with engine.connect() as conn:
        query_map = text(f"""
            SELECT state_code, SUM(loan_count) as loan_count
            FROM {MART_SCHEMA}.prd_risk_summary
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
    selected_year   = st.sidebar.selectbox("Year", nav_years)
    selected_states = st.sidebar.multiselect("States", nav_states, default=nav_states[:5] if nav_states else [])

    if not selected_states:
        st.warning("Please select at least one state.")
        st.stop()

    # 3. Load Data with Predicate Pushdown based on user selection
    df_summary, df_sample = load_main_data(selected_year, selected_states)
    df_map = load_heatmap_data(selected_year)  # Heatmap retrieves full states for selected year

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
        avg_rate    = df_summary['sum_interest_rate'].sum() / total_loans if not df_summary.empty and total_loans > 0 else 0
        st.metric("Avg Market Rate", f"{avg_rate:.2f}%")
    with m4:
        total_loans = df_summary['loan_count'].sum()
        hr_cnt  = df_summary[df_summary['risk_segment'] == 'High']['loan_count'].sum()
        hr_pct  = (hr_cnt / total_loans * 100) if not df_summary.empty and total_loans > 0 else 0
        st.metric("High Risk %", f"{hr_pct:.1f}%")

    # --- Visualization Row 1 ---
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("📊 Risk Hierarchy")
        if not df_summary.empty:
            fig_sun = px.sunburst(df_summary, path=['risk_segment', 'loan_purpose_name'], values='loan_count',
                                color='risk_segment', color_discrete_map=risk_colors)
            st.plotly_chart(fig_sun, use_container_width=True)
        else:
            st.info("No data available")
    with c2:
        st.subheader("📍 Geography Heatmap")
        if not df_map.empty:
            fig_map = px.choropleth(df_map, locations='state_code', locationmode="USA-states",
                                    color='loan_count', scope="usa", color_continuous_scale="Blues")
            fig_map.add_scattergeo(
                locations=df_map['state_code'],
                locationmode="USA-states",
                text=df_map['state_code'],
                mode="text",
                textfont=dict(color="black", size=10, weight="bold"),
                hoverinfo="skip"
            )
            fig_map.update_layout(margin={"r": 0, "t": 0, "l": 0, "b": 0})
            st.plotly_chart(fig_map, use_container_width=True)
        else:
            st.info("No data available for this year")

    # --- Visualization Row 2 ---
    c3, c4 = st.columns([1.2, 0.8])
    with c3:
        st.subheader("💰 Income vs Rate Spread (Sampled)")
        if not df_sample.empty:
            fig_scatter = px.scatter(df_sample, x='income', y='interest_rate_spread',
                                     color='risk_segment', size='loan_amount',
                                     color_discrete_map=risk_colors, opacity=0.5)
            fig_scatter.update_xaxes(showgrid=False)
            fig_scatter.update_yaxes(showgrid=False)
            st.plotly_chart(fig_scatter, use_container_width=True)
        else:
            st.info("No data available")
    with c4:
        st.subheader("🏦 Purpose Breakdown")
        if not df_summary.empty:
            grouped  = df_summary.groupby(['loan_purpose_name', 'risk_segment'])['loan_count'].sum().reset_index()
            fig_bar  = px.bar(grouped, x='loan_purpose_name', y='loan_count',
                              color='risk_segment', barmode='group', color_discrete_map=risk_colors)
            st.plotly_chart(fig_bar, use_container_width=True)
        else:
            st.info("No data available")

except Exception as e:
    st.error(f"Pipeline Status: Pending. Error: {e}")