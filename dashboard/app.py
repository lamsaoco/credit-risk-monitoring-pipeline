import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os

# Page configuration
st.set_page_config(
    page_title="Credit Risk Monitoring Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Dark mode custom CSS
st.markdown("""
    <style>
    .main {
        background-color: #0e1117;
    }
    .stMetric {
        background-color: #161b22;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #30363d;
    }
    </style>
    """, unsafe_allow_html=True)

# Database connection
@st.cache_resource
def get_engine():
    user = os.getenv("DW_POSTGRES_USER", "dw_user")
    password = os.getenv("DW_POSTGRES_PASSWORD", "dw_secure_pass")
    host = os.getenv("DW_POSTGRES_HOST", "postgres-dw")
    port = os.getenv("DW_POSTGRES_PORT", "5432")
    db = os.getenv("DW_POSTGRES_DB", "credit_risk_dw")
    return create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")

engine = get_engine()

# Data loading
@st.cache_data(ttl=600)
def load_data():
    query = "SELECT * FROM credit_risk_prod.fct_loan_risk"
    df = pd.read_sql(query, engine)
    return df

try:
    df = load_data()
    
    # Sidebar Filters
    st.sidebar.header("🔍 Filters")
    
    # Year filter (only available years)
    available_years = sorted(df['data_year'].unique().tolist(), reverse=True)
    selected_year = st.sidebar.selectbox("Select Year", available_years)
    
    # State filter
    available_states = sorted(df['state_name'].dropna().unique().tolist())
    selected_states = st.sidebar.multiselect("Select States", available_states, default=available_states[:5] if available_states else [])

    # Risk segment filter
    risk_segments = df['risk_segment'].unique().tolist()
    selected_risk = st.sidebar.multiselect("Risk Segment", risk_segments, default=risk_segments)

    # Filtering data
    filtered_df = df[df['data_year'] == selected_year]
    if selected_states:
        filtered_df = filtered_df[filtered_df['state_name'].isin(selected_states)]
    filtered_df = filtered_df[filtered_df['risk_segment'].isin(selected_risk)]

    # --- Header ---
    st.title(f"🚀 HMDA Credit Risk Dashboard - {selected_year}")
    st.markdown("---")

    # --- Top Metrics ---
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Loans", f"{len(filtered_df):,}")
    with m2:
        total_val = filtered_df['loan_amount'].sum()
    with m3:
        avg_rate = filtered_df['interest_rate'].mean()
        st.metric("Avg Interest Rate", f"{avg_rate:.2f}%")
    with m4:
        high_risk_pct = (len(filtered_df[filtered_df['risk_segment'] == 'High']) / len(filtered_df) * 100) if len(filtered_df) > 0 else 0
        st.metric("High Risk %", f"{high_risk_pct:.1f}%", delta_color="inverse")

    st.markdown("###")

    # --- Charts Row 1 ---
    c1, c2 = st.columns([1, 1])

    with c1:
        st.subheader("📊 Risk Segmentation Distribution")
        risk_counts = filtered_df['risk_segment'].value_counts().reset_index()
        risk_counts.columns = ['Segment', 'Count']
        fig_risk = px.pie(
            risk_counts, 
            values='Count', 
            names='Segment',
            color='Segment',
            color_discrete_map={'High': '#ff4b4b', 'Medium': '#ffa421', 'Low': '#00d4ff'},
            hole=0.4
        )
        st.plotly_chart(fig_risk, use_container_width=True)

    with c2:
        st.subheader("📍 Geographical Distribution (Heatmap)")
        state_counts = filtered_df.groupby('state_code')['loan_amount'].count().reset_index()
        fig_map = px.choropleth(
            state_counts,
            locations='state_code',
            locationmode="USA-states",
            color='loan_amount',
            scope="usa",
            color_continuous_scale="Viridis",
            labels={'loan_amount': 'Loan Count'}
        )
        fig_map.update_layout(geo=dict(bgcolor='rgba(0,0,0,0)'))
        st.plotly_chart(fig_map, use_container_width=True)

    # --- Charts Row 2 ---
    st.markdown("---")
    c3, c4 = st.columns([1, 1])

    with c3:
        st.subheader("💰 Income vs Interest Rate Spread")
        fig_scatter = px.scatter(
            filtered_df,
            x='income',
            y='interest_rate_spread',
            color='risk_segment',
            size='loan_amount',
            hover_data=['loan_purpose_name'],
            labels={'income': 'Income ($k)', 'interest_rate_spread': 'Rate Spread (%)'},
            color_discrete_map={'High': '#ff4b4b', 'Medium': '#ffa421', 'Low': '#00d4ff'}
        )
        st.plotly_chart(fig_scatter, use_container_width=True)

    with c4:
        st.subheader("🏦 Loan Purpose Breakdown")
        purpose_counts = filtered_df.groupby(['loan_purpose_name', 'risk_segment']).size().reset_index(name='count')
        fig_bar = px.bar(
            purpose_counts,
            x='loan_purpose_name',
            y='count',
            color='risk_segment',
            barmode='group',
            color_discrete_map={'High': '#ff4b4b', 'Medium': '#ffa421', 'Low': '#00d4ff'}
        )
        st.plotly_chart(fig_bar, use_container_width=True)

    # --- Data Table ---
    with st.expander("📝 View Raw Filtered Data"):
        st.dataframe(filtered_df.head(100))

except Exception as e:
    st.error(f"Waiting for dbt transformation to finish or Database connection issue...")
    st.info("Ensure you have run the dbt build DAG to populate the 'credit_risk_prod.fct_loan_risk' table.")
    if st.checkbox("Show Error Detail"):
        st.write(e)
