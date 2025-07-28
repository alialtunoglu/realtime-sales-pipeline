"""
Bronze Layer - Data Ingestion Task
CSV'den Delta formatına veri yükleme
"""
import os
import sys
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def run_bronze_ingestion():
    """CSV verisini Bronze katmanına Delta formatında yükler"""
    
    # Spark session
    builder = SparkSession.builder \
        .appName("RetailCSVIngestion") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Proje kök dizinini bul (Airflow için absolute path kullan)
    project_root = '/Users/alialtunoglu/Desktop/realtime-sales-pipeline'
    
    try:
        # CSV'den oku (absolute path)
        input_path = os.path.join(project_root, "data/input/retail_data.csv")
        print(f"🔍 Input path: {input_path}")
        
        # Path kontrolü
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"CSV dosyası bulunamadı: {input_path}")
            
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(input_path)
        
        print(f"📊 Okunan veri satır sayısı: {df.count()}")
        df.printSchema()
        
        # Bronze'a Delta formatında yaz (absolute path)
        output_path = os.path.join(project_root, "delta/bronze/online_retail")
        df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(output_path)
        
        print("✅ Bronze ingestion başarıyla tamamlandı.")
        print(f"📁 Veri kaydedildi: {output_path}")
        
    except Exception as e:
        print(f"❌ Bronze ingestion hatası: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    run_bronze_ingestion()
