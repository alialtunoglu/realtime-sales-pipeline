import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import matplotlib.pyplot as plt
import plotly.express as px

# Spark session
builder = SparkSession.builder \
    .appName("Dashboard") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

st.title("🛒 Retail Sales Dashboard")
st.write("**Medallion Architecture** ile oluşturulmuş modern veri pipeline'ı dashboard'u")

# DAILY SALES
df_daily = spark.read.format("delta").load("delta/gold/daily_sales").toPandas()
st.subheader("📈 Günlük Satış Cirosu")
if not df_daily.empty:
    fig_daily = px.line(df_daily, x='SaleDate', y='TotalRevenue', 
                       title='Günlük Toplam Ciro')
    st.plotly_chart(fig_daily)
    st.write(f"📊 **Toplam Günlük Kayıt:** {len(df_daily)} gün")
else:
    st.warning("⚠️ Günlük satış verisi bulunamadı")

# TOP PRODUCTS
df_top = spark.read.format("delta").load("delta/gold/top_products").toPandas()
st.subheader("🏆 En Çok Satılan Ürünler (Top 10)")
if not df_top.empty:
    fig_products = px.bar(df_top.head(10), x='TotalSold', y='Description', 
                         orientation='h', title='En Çok Satılan Ürünler')
    st.plotly_chart(fig_products)
    st.write(f"📦 **Toplam Ürün Çeşidi:** {len(df_top)}")
else:
    st.warning("⚠️ Ürün satış verisi bulunamadı")

# COUNTRY SALES
df_country = spark.read.format("delta").load("delta/gold/country_sales").toPandas()
st.subheader("🌍 Ülke Bazlı Satış Dağılımı (Top 10)")
if not df_country.empty:
    fig_country = px.bar(df_country.head(10), x='Country', y='CountryRevenue',
                        title='Ülke Bazlı Toplam Ciro')
    st.plotly_chart(fig_country)
    st.write(f"🌎 **Toplam Ülke:** {len(df_country)} ülke")
else:
    st.warning("⚠️ Ülke bazlı satış verisi bulunamadı")

# RFM SEGMENTATION
st.subheader("🎯 RFM Analizi (Müşteri Segmentasyonu)")
try:
    df_rfm = spark.read.format("delta").load("delta/gold/rfm_table").toPandas()
    if not df_rfm.empty:
        st.write("**En Değerli 20 Müşteri (Monetary Bazında):**")
        st.dataframe(df_rfm.sort_values("Monetary", ascending=False).head(20))
        
        # RFM skorları için histogramlar
        col1, col2, col3 = st.columns(3)
        with col1:
            st.metric("Toplam Müşteri", len(df_rfm))
        with col2:
            st.metric("Ort. Monetary", f"${df_rfm['Monetary'].mean():.2f}")
        with col3:
            st.metric("Ort. Frequency", f"{df_rfm['Frequency'].mean():.1f}")
    else:
        st.warning("⚠️ RFM verisi bulunamadı")
except Exception as e:
    st.error(f"❌ RFM verisini okuma hatası: {str(e)}")

# CLTV ANALYSIS
st.subheader("💰 CLTV (Customer Lifetime Value) Analizi")
try:
    df_cltv = spark.read.format("delta").load("delta/gold/cltv_table").toPandas()
    if not df_cltv.empty:
        st.write("**En Yüksek CLTV'ye Sahip 20 Müşteri:**")
        st.dataframe(df_cltv.sort_values("CLTV", ascending=False).head(20))
        
        # CLTV dağılımı
        fig_cltv = px.histogram(df_cltv, x='CLTV', nbins=50, 
                               title='CLTV Dağılımı')
        st.plotly_chart(fig_cltv)
    else:
        st.warning("⚠️ CLTV verisi bulunamadı")
except Exception as e:
    st.error(f"❌ CLTV verisini okuma hatası: {str(e)}")

# FORECASTING
st.subheader("🔮 12 Aylık Gelir Tahmini (Forecasting)")
try:
    df_forecast = spark.read.format("delta").load("delta/gold/forecast_table").toPandas()
    if not df_forecast.empty:
        st.write("**En Yüksek Tahmini Gelire Sahip 20 Müşteri:**")
        st.dataframe(df_forecast.sort_values("ExpectedRevenue_12M", ascending=False).head(20))
        
        # Tahmin özeti
        total_forecast = df_forecast['ExpectedRevenue_12M'].sum()
        st.success(f"🎯 **Toplam 12 Aylık Tahmini Gelir:** ${total_forecast:,.2f}")
        
        # Forecast dağılımı
        fig_forecast = px.histogram(df_forecast, x='ExpectedRevenue_12M', nbins=50,
                                   title='12 Aylık Tahmini Gelir Dağılımı')
        st.plotly_chart(fig_forecast)
    else:
        st.warning("⚠️ Forecasting verisi bulunamadı")
except Exception as e:
    st.error(f"❌ Forecasting verisini okuma hatası: {str(e)}")

# FOOTER
st.markdown("---")
st.markdown("""
### 🏗️ Pipeline Mimarisi
- **🥉 Bronze Layer:** Ham CSV verisi → Delta format
- **🥈 Silver Layer:** Veri temizleme ve validasyon
- **🥇 Gold Layer:** İş analitiği tabloları
- **🔬 Advanced Analytics:** RFM, CLTV, Forecasting

### 📊 Dashboard Bilgileri
- **Veri Kaynağı:** Online Retail Dataset
- **İşleme Motoru:** Apache Spark + Delta Lake
- **Orchestration:** Apache Airflow
- **Visualization:** Streamlit + Plotly
""")

# Spark session'ı kapat
spark.stop()