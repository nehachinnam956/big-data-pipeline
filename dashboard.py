import streamlit as st
import pandas as pd
import plotly.express as px

# Load data from CSV
df = pd.read_csv('olist_results.csv')
df.columns = df.columns.str.strip()

# Convert types
df['total_orders'] = pd.to_numeric(df['total_orders'], errors='coerce')
df['revenue'] = pd.to_numeric(df['revenue'], errors='coerce')
df['late_shipments'] = pd.to_numeric(df['late_shipments'], errors='coerce')
df['period'] = df['order_year'].astype(str) + '-' + df['order_month'].astype(str).str.zfill(2)
df['late_rate'] = (df['late_shipments'] / df['total_orders'] * 100).round(1)

# Page config
st.set_page_config(page_title="Olist Analytics", page_icon="🛒", layout="wide")
st.title("🛒 Brazilian E-Commerce Analytics Pipeline")
st.caption("AWS S3 + PySpark on EMR + AWS Athena + Streamlit")

# KPI Cards
st.markdown("### Key Metrics")
c1, c2, c3, c4 = st.columns(4)
c1.metric("Total Orders", f"{int(df['total_orders'].sum()):,}")
c2.metric("Total Revenue", f"${df['revenue'].sum():,.0f}")
c3.metric("Late Shipments", f"{int(df['late_shipments'].sum()):,}")
c4.metric("Peak Month", df.loc[df['total_orders'].idxmax(), 'period'])

st.divider()

# Charts row 1
col1, col2 = st.columns(2)

with col1:
    st.markdown("### Monthly Revenue Trend")
    fig = px.line(df, x='period', y='revenue',
                  markers=True, color_discrete_sequence=['#2563EB'])
    fig.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.markdown("### Monthly Order Volume")
    fig2 = px.bar(df, x='period', y='total_orders',
                  color_discrete_sequence=['#16A34A'])
    fig2.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig2, use_container_width=True)

# Charts row 2
col3, col4 = st.columns(2)

with col3:
    st.markdown("### Late Shipments Over Time")
    fig3 = px.bar(df, x='period', y='late_shipments',
                  color_discrete_sequence=['#DC2626'])
    fig3.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig3, use_container_width=True)

with col4:
    st.markdown("### Late Shipment Rate %")
    fig4 = px.line(df, x='period', y='late_rate',
                   markers=True, color_discrete_sequence=['#D97706'])
    fig4.update_layout(xaxis_tickangle=-45)
    st.plotly_chart(fig4, use_container_width=True)

st.divider()

# Data table
st.markdown("### Monthly Data Table")
st.dataframe(df[['period','total_orders','revenue',
                  'late_shipments','late_rate']],
             use_container_width=True)

st.divider()

# Architecture
st.markdown("### Pipeline Architecture")
st.code("""
Raw CSVs (9 files, 100K+ orders from Olist)
         ↓
AWS S3 Bronze Layer
         ↓
PySpark on AWS EMR — joined 5 tables, computed metrics
         ↓
AWS S3 Silver Layer (Parquet, partitioned by year/month)
         ↓
AWS Athena — serverless SQL analytics
         ↓
Streamlit Dashboard
""")