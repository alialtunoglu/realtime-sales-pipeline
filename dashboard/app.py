import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import matplotlib.pyplot as plt

# Spark session
builder = SparkSession.builder \
    .appName("Dashboard") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

st.title("🛒 Retail Sales Dashboard")

# DAILY SALES
df_daily = spark.read.format("delta").load("delta/gold/daily_sales").toPandas()
st.subheader("📈 Günlük Satış Cirosu")
st.line_chart(df_daily.set_index("SaleDate")[["TotalRevenue"]])

# TOP PRODUCTS
df_top = spark.read.format("delta").load("delta/gold/top_products").toPandas()
st.subheader("🏆 En Çok Satılan Ürünler")
st.bar_chart(df_top.set_index("Description")[["TotalSold"]])

# COUNTRY SALES
df_country = spark.read.format("delta").load("delta/gold/country_sales").toPandas()
st.subheader("🌍 Ülke Bazlı Satış Dağılımı")
st.bar_chart(df_country.set_index("Country")[["CountryRevenue"]])

# RFM SEGMENTATION
df_rfm = spark.read.format("delta").load("delta/gold/rfm_table").toPandas()
st.subheader("📊 RFM Analizi (Recency - Frequency - Monetary)")
st.dataframe(df_rfm.sort_values("Monetary", ascending=False).head(20))

# CLTV ANALYSIS
df_cltv = spark.read.format("delta").load("delta/gold/cltv_table").toPandas()
st.subheader("💰 CLTV (Customer Lifetime Value) Analizi")
st.dataframe(df_cltv.sort_values("CLTV", ascending=False).head(20))