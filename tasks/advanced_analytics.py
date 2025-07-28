"""
Advanced Analytics Tasks: RFM, CLTV ve Forecasting
Silver veriden ileri düzey müşteri analitiği
"""
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, max as spark_max, count, sum as spark_sum, datediff, to_date, lit, expr

def run_advanced_analytics():
    """RFM, CLTV ve Forecasting analitiklerini çalıştırır"""
    
    # Spark session
    builder = SparkSession.builder \
        .appName("AdvancedAnalytics") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Proje kök dizinini bul (Airflow için absolute path kullan)
    project_root = '/Users/alialtunoglu/Desktop/realtime-sales-pipeline'
    
    try:
        # Silver verisini yükle (absolute path)
        silver_path = os.path.join(project_root, "delta/silver/online_retail_cleaned")
        print(f"🔍 Silver path: {silver_path}")
        
        # Path kontrolü
        if not os.path.exists(silver_path):
            raise FileNotFoundError(f"Silver tablo bulunamadı: {silver_path}")
            
        df_silver = spark.read.format("delta").load(silver_path)
        
        print(f"📊 Silver veri satır sayısı: {df_silver.count()}")
        
        # 1️⃣ RFM ANALİZİ
        print("🔄 RFM analizi hesaplanıyor...")
        
        # En son tarih
        latest_date = df_silver.agg(spark_max("InvoiceDate")).collect()[0][0]
        print(f"📅 En son tarih: {latest_date}")
        
        # RFM hesaplama
        rfm = df_silver \
            .withColumn("InvoiceDateOnly", to_date("InvoiceDate")) \
            .groupBy("CustomerID") \
            .agg(
                datediff(lit(latest_date), spark_max("InvoiceDateOnly")).alias("Recency"),
                count("InvoiceNo").alias("Frequency"),
                spark_sum(col("Quantity") * col("UnitPrice")).alias("Monetary")
            )
        
        print(f"📊 RFM müşteri sayısı: {rfm.count()}")
        
        # RFM kaydet
        rfm_path = os.path.join(project_root, "delta/gold/rfm_table")
        rfm.write.format("delta").mode("overwrite").save(rfm_path)
        
        # 2️⃣ CLTV ANALİZİ
        print("🔄 CLTV analizi hesaplanıyor...")
        
        # CLTV hesapla (basit formül)
        cltv = rfm.withColumn(
            "CLTV", 
            (col("Frequency") * col("Monetary")) / (col("Recency") + lit(1))
        )
        
        # CLTV kaydet
        cltv_path = os.path.join(project_root, "delta/gold/cltv_table")
        cltv.write.format("delta").mode("overwrite").save(cltv_path)
        
        # 3️⃣ FORECASTİNG
        print("🔄 Forecasting analizi hesaplanıyor...")
        
        # 12 aylık gelir tahmini
        forecast = cltv.withColumn("ExpectedRevenue_12M", col("CLTV") * lit(12))
        
        # Forecast kaydet
        forecast_path = os.path.join(project_root, "delta/gold/forecast_table")
        forecast.write.format("delta").mode("overwrite").save(forecast_path)
        
        print("✅ İleri düzey analitik işlemleri tamamlandı!")
        print("📊 Oluşturulan tablolar:")
        print("   🎯 rfm_table (Müşteri Segmentasyonu)")
        print("   💰 cltv_table (Müşteri Yaşam Boyu Değeri)")
        print("   🔮 forecast_table (12 Aylık Gelir Tahmini)")
        
        # Örnek veriler
        print("\n🎯 RFM Analizi (Top 5):")
        rfm.orderBy(col("Monetary").desc()).show(5)
        
        print("\n💰 CLTV Analizi (Top 5):")
        cltv.orderBy(col("CLTV").desc()).show(5)
        
        print("\n🔮 Forecasting (Top 5 Tahmini):")
        forecast.orderBy(col("ExpectedRevenue_12M").desc()).show(5)
        
    except Exception as e:
        print(f"❌ Advanced analytics hatası: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    run_advanced_analytics()
