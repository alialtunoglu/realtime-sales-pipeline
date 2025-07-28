"""
Silver Layer - Data Cleaning Task
Bronze'dan temizlenmiş veriyi Silver katmanına yükler
"""
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import col, to_timestamp, when, trim

def run_silver_cleaning():
    """Bronze verisini temizleyip Silver katmanına yükler"""
    
    # Spark session
    builder = SparkSession.builder \
        .appName("SilverCleaning") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Proje kök dizinini bul (Airflow için absolute path kullan)
    project_root = '/Users/alialtunoglu/Desktop/realtime-sales-pipeline'
    
    try:
        # Bronze veriyi oku (absolute path)
        bronze_path = os.path.join(project_root, "delta/bronze/online_retail")
        print(f"🔍 Bronze path: {bronze_path}")
        
        # Path kontrolü
        if not os.path.exists(bronze_path):
            raise FileNotFoundError(f"Bronze tablo bulunamadı: {bronze_path}")
            
        df_bronze = spark.read.format("delta").load(bronze_path)
        
        print(f"📊 Bronze veri satır sayısı: {df_bronze.count()}")
        
        # 📌 Veri temizleme adımları
        df_silver = df_bronze \
            .dropna(subset=["InvoiceNo", "StockCode", "Description", "InvoiceDate", "CustomerID"]) \
            .withColumn("InvoiceDate", to_timestamp(col("InvoiceDate"))) \
            .withColumn("Quantity", col("Quantity").cast("int")) \
            .withColumn("UnitPrice", col("UnitPrice").cast("double")) \
            .withColumn("CustomerID", col("CustomerID").cast("string")) \
            .withColumn("Description", trim(col("Description"))) \
            .filter(col("Quantity") > 0) \
            .filter(col("UnitPrice") > 0)
        
        print(f"📊 Temizlenmiş veri satır sayısı: {df_silver.count()}")
        
        # Silver katmanına yaz (absolute path)
        silver_path = os.path.join(project_root, "delta/silver/online_retail_cleaned")
        df_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_path)
        
        print("✅ Silver cleaning işlemi tamamlandı.")
        print(f"📁 Veri kaydedildi: {silver_path}")
        
        # Örnek veri göster
        df_silver.show(5)
        
    except Exception as e:
        print(f"❌ Silver cleaning hatası: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    run_silver_cleaning()
