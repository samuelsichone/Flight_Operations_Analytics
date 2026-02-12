import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# ---------------------------
# PAGE CONFIG
# ---------------------------
st.set_page_config(
    page_title="Flights Operations Dashboard",
    layout="wide",
    page_icon="✈️"
)

# ---------------------------
# CUSTOM CSS STYLE
# ---------------------------
st.markdown("""
<style>
.main {
    background-color: #0e1117;
}
.metric-card {
    background-color: #161b22;
    padding: 20px;
    border-radius: 12px;
    text-align: center;
    box-shadow: 0px 0px 12px rgba(0,0,0,0.3);
}
</style>
""", unsafe_allow_html=True)

st.title("✈️ Flights Operations Command Center")

# ---------------------------
# CONNECT TO SNOWFLAKE
# ---------------------------
session = get_active_session()

# ---------------------------
# LOAD DATA
# ---------------------------
query = """
SELECT
    WINDOW_START,
    ORIGIN_COUNTRY,
    TOTAL_FLIGHTS,
    AVG_VELOCITY,
    ON_GROUND
FROM KPI.FLIGHT_KPIS
ORDER BY WINDOW_START DESC
"""

df = session.sql(query).to_pandas()

if df.empty:
    st.warning("No data found in FLIGHT_KPIS table.")
    st.stop()

df["WINDOW_START"] = pd.to_datetime(df["WINDOW_START"])

# ---------------------------
# SIDEBAR
# ---------------------------
st.sidebar.title("🧭 Control Panel")

countries = st.sidebar.multiselect(
    "🌍 Origin Country",
    options=df["ORIGIN_COUNTRY"].unique(),
    default=df["ORIGIN_COUNTRY"].unique()
)

date_range = st.sidebar.date_input(
    "📅 Date Range",
    value=(df["WINDOW_START"].min(), df["WINDOW_START"].max())
)

filtered = df[
    (df["ORIGIN_COUNTRY"].isin(countries)) &
    (df["WINDOW_START"].dt.date >= date_range[0]) &
    (df["WINDOW_START"].dt.date <= date_range[1])
]

# ---------------------------
# KPI ROW
# ---------------------------
st.subheader("📊 Operational KPIs")

k1, k2, k3 = st.columns(3)

with k1:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("✈️ Total Flights", int(filtered["TOTAL_FLIGHTS"].sum()))
    st.markdown('</div>', unsafe_allow_html=True)

with k2:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("⚡ Avg Velocity", round(filtered["AVG_VELOCITY"].mean(), 2))
    st.markdown('</div>', unsafe_allow_html=True)

with k3:
    st.markdown('<div class="metric-card">', unsafe_allow_html=True)
    st.metric("🛬 On Ground", int(filtered["ON_GROUND"].sum()))
    st.markdown('</div>', unsafe_allow_html=True)

# ---------------------------
# TABS LAYOUT
# ---------------------------
tab1, tab2, tab3 = st.tabs([
    "📈 Flight Trends",
    "🌍 Country Analysis",
    "📋 Raw Data"
])

# ---------------------------
# TAB 1 - TIME SERIES
# ---------------------------
with tab1:
    st.subheader("Flights Over Time")

    flights_time = (
        filtered.groupby("WINDOW_START")["TOTAL_FLIGHTS"]
        .sum()
        .reset_index()
    )

    st.line_chart(flights_time.set_index("WINDOW_START"))

# ---------------------------
# TAB 2 - COUNTRY
# ---------------------------
with tab2:
    st.subheader("Flights by Country")

    country_data = (
        filtered.groupby("ORIGIN_COUNTRY")["TOTAL_FLIGHTS"]
        .sum()
        .reset_index()
    )

    st.bar_chart(country_data.set_index("ORIGIN_COUNTRY"))

# ---------------------------
# TAB 3 - DATA
# ---------------------------
with tab3:
    st.subheader("Detailed Data")
    st.dataframe(filtered, use_container_width=True)
