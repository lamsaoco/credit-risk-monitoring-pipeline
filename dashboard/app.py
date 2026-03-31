import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine
import os

# --- 1. Page Configuration & Professional UI ---
st.set_page_config(
    page_title="Credit Risk Monitoring Pipeline",
    page_icon="🛡️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Dark mode custom CSS (Kết hợp bản cũ của bạn)
st.markdown("""
    <style>
    .main { background-color: #0e1117; }
    .stMetric {
        background-color: #161b22;
        padding: 20px;
        border-radius: 12px;
        border: 1px solid #30363d;
        box-shadow: 0 4px 6px rgba(0,0,0,0.1);
    }
    [data-testid="stMetricValue"] { font-size: 28px; color: #00d4ff; }
    </style>
    """, unsafe_allow_html=True)

# --- 2. Database connection (Secure & Optimized) ---
@st.cache_resource
def get_engine():
    user = os.getenv("DW_POSTGRES_USER")
    password = os.getenv("DW_POSTGRES_PASSWORD")
    host = os.getenv("DW_POSTGRES_HOST")
    db = os.getenv("DW_POSTGRES_DB")
    # Tự động báo lỗi nếu thiếu biến môi trường
    if not all([user, password, host, db]):
        st.error("⚠️ Environment Variables NOT found!")
        st.stop()
    return create_engine(f"postgresql://{user}:{password}@{host}:5432/{db}")

engine = get_engine()

# --- 3. Data Loading (The dbt Way) ---
@st.cache_data(ttl=600)
def load_all_data():
    # Chỉ load 2 bảng đã nấu sẵn, tổng cộng chưa tới 15k dòng
    df_summ = pd.read_sql("SELECT * FROM product.prd_risk_summary", engine)
    df_samp = pd.read_sql("SELECT * FROM product.prd_loan_sample", engine)
    return df_summ, df_samp

try:
    df_summary, df_sample = load_all_data()

    # --- 4. Sidebar Filters ---
    st.sidebar.header("🔍 Filter Analytics")
    st.sidebar.markdown("---")
    
    available_years = sorted(df_summary['data_year'].unique().tolist(), reverse=True)
    selected_year = st.sidebar.selectbox("📅 Processing Year", available_years)
    
    available_states = sorted(df_summary['state_name'].dropna().unique().tolist())
    selected_states = st.sidebar.multiselect("📍 Geography (States)", available_states, default=available_states[:5])

    risk_segments = df_summary['risk_segment'].unique().tolist()
    selected_risk = st.sidebar.multiselect("⚠️ Risk Level", risk_segments, default=risk_segments)

    # Filtering Logic (Bản kết hợp)
    mask_summ = (df_summary['data_year'] == selected_year) & \
                (df_summary['state_name'].isin(selected_states)) & \
                (df_summary['risk_segment'].isin(selected_risk))
    
    mask_samp = (df_sample['data_year'] == selected_year) & \
                (df_sample['state_name'].isin(selected_states)) & \
                (df_sample['risk_segment'].isin(selected_risk))

    filt_summ = df_summary[mask_summ]
    filt_samp = df_sample[mask_samp]

    # --- 5. Header Section ---
    st.title(f"🚀 Credit Risk Monitoring Pipeline")
    st.subheader(f"Analyzing {selected_year} Mortgage Data")
    st.markdown("---")

    # --- 6. Top Metrics Row ---
    m1, m2, m3, m4 = st.columns(4)
    with m1:
        st.metric("Total Applications", f"{filt_summ['loan_count'].sum():,}")
    with m2:
        total_val = filt_summ['total_loan_amount'].sum()
        st.metric("Portfolio Value", f"${total_val/1e6:.1f}M")
    with m3:
        # Weighted average for accuracy
        avg_rate = filt_summ['sum_interest_rate'].sum() / filt_summ['loan_count'].sum() if not filt_summ.empty else 0
        st.metric("Avg Market Rate", f"{avg_rate:.2f}%")
    with m4:
        hr_cnt = filt_summ[filt_summ['risk_segment'] == 'High']['loan_count'].sum()
        total_cnt = filt_summ['loan_count'].sum()
        hr_pct = (hr_cnt / total_cnt * 100) if total_cnt > 0 else 0
        st.metric("High Risk Concentration", f"{hr_pct:.1f}%", delta="-2.1%" if hr_pct > 20 else "+0.5%")

    st.markdown("###")

    # --- 7. Main Visuals (Row 1) ---
    c1, c2 = st.columns([1, 1])
    with c1:
        st.subheader("📊 Risk Hierarchy Breakdown")
        # Dùng Sunburst cho chuyên nghiệp hơn bản cũ
        fig_sun = px.sunburst(filt_summ, path=['risk_segment', 'loan_purpose_name'], 
                             values='loan_count',
                             color='risk_segment',
                             color_discrete_map={'High': '#ff4b4b', 'Medium': '#ffa421', 'Low': '#00d4ff'})
        fig_sun.update_layout(margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(fig_sun, use_container_width=True)

    with c2:
        st.subheader("📍 State-level Loan Volume")
        map_df = filt_summ.groupby('state_code')['loan_count'].sum().reset_index()
        fig_map = px.choropleth(map_df, locations='state_code', locationmode="USA-states",
                                color='loan_count', scope="usa", color_continuous_scale="Viridis")
        fig_map.update_layout(geo=dict(bgcolor='rgba(0,0,0,0)'), margin=dict(t=0, l=0, r=0, b=0))
        st.plotly_chart(fig_map, use_container_width=True)

    # --- 8. Detailed Analysis (Row 2) ---
    st.markdown("---")
    c3, c4 = st.columns([1.2, 0.8])
    with c3:
        st.subheader("💰 Income vs Rate Spread (Sampled)")
        fig_scatter = px.scatter(filt_samp, x='income', y='interest_rate_spread', 
                                 color='risk_segment', size='loan_amount', opacity=0.7,
                                 color_discrete_map={'High': '#ff4b4b', 'Medium': '#ffa421', 'Low': '#00d4ff'})
        st.plotly_chart(fig_scatter, use_container_width=True)

    with c4:
        st.subheader("🏦 Purpose vs Risk Count")
        fig_bar = px.bar(filt_summ.groupby(['loan_purpose_name', 'risk_segment'])['loan_count'].sum().reset_index(),
                         x='loan_purpose_name', y='loan_count', color='risk_segment',
                         barmode='group', color_discrete_map={'High': '#ff4b4b', 'Medium': '#ffa421', 'Low': '#00d4ff'})
        st.plotly_chart(fig_bar, use_container_width=True)

    # --- 9. Data Table Footer ---
    with st.expander("📝 View Raw Sample Data (Top 50 rows)"):
        st.dataframe(filt_samp.head(50), use_container_width=True)

except Exception as e:
    st.error(f"Waiting for Data Pipeline... Error: {e}")