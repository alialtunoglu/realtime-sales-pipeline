"""
Gold Layer - Aggregation & Analytics Task
Silver'dan analiz tablolarını oluşturur
"""
import os
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from pyspark.sql.functions import sum, count, to_date, col, avg, countDistinct

def run_gold_aggregation():
    """Silver verisinden Gold katmanı analiz tablolarını oluşturur"""
    
    # Spark session
    builder = SparkSession.builder \
        .appName("GoldAggregation") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    # Proje kök dizinini bul (Airflow için absolute path kullan)
    project_root = '/Users/alialtunoglu/Desktop/realtime-sales-pipeline'
    
    try:
        # Silver'dan oku (absolute path)
        silver_path = os.path.join(project_root, "delta/silver/online_retail_cleaned")
        print(f"🔍 Silver path: {silver_path}")
        
        # Path kontrolü
        if not os.path.exists(silver_path):
            raise FileNotFoundError(f"Silver tablo bulunamadı: {silver_path}")
            
        df_silver = spark.read.format("delta").load(silver_path)
        
        print(f"📊 Silver veri satır sayısı: {df_silver.count()}")
        
        # GOLD #1: Günlük satış toplamı
        print("🔄 Günlük satış agregasyonu hesaplanıyor...")
        daily_sales = df_silver \
            .withColumn("SaleDate", to_date("InvoiceDate")) \
            .groupBy("SaleDate") \
            .agg(
                sum("Quantity").alias("TotalQuantity"),
                sum(df_silver["Quantity"] * df_silver["UnitPrice"]).alias("TotalRevenue"),
                countDistinct("CustomerID").alias("UniqueCustomers"),
                count("InvoiceNo").alias("TotalTransactions")
            ) \
            .orderBy("SaleDate")
        
        # GOLD #2: En çok satılan ürünler (top 20)
        print("🔄 En çok satılan ürünler hesaplanıyor...")
        top_products = df_silver \
            .groupBy("StockCode", "Description") \
            .agg(
                sum("Quantity").alias("TotalSold"),
                sum(df_silver["Quantity"] * df_silver["UnitPrice"]).alias("TotalRevenue"),
                avg("UnitPrice").alias("AvgPrice")
            ) \
            .orderBy(col("TotalSold").desc()) \
            .limit(20)
        
        # GOLD #3: Ülke bazlı satış
        print("🔄 Ülke bazlı satış hesaplanıyor...")
        country_sales = df_silver \
            .groupBy("Country") \
            .agg(
                sum(df_silver["Quantity"] * df_silver["UnitPrice"]).alias("CountryRevenue"),
                sum("Quantity").alias("TotalQuantity"),
                countDistinct("CustomerID").alias("UniqueCustomers"),
                countDistinct("StockCode").alias("UniqueProducts")
            ) \
            .orderBy(col("CountryRevenue").desc())
        
        # Kaydet
        print("💾 Gold tabloları kaydediliyor...")
        daily_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            os.path.join(project_root, "delta/gold/daily_sales")
        )
        top_products.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            os.path.join(project_root, "delta/gold/top_products")
        )
        country_sales.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(
            os.path.join(project_root, "delta/gold/country_sales")
        )
        
        print("✅ Gold aggregation işlemi tamamlandı.")
        print("📊 Oluşturulan tablolar:")
        print("   📈 daily_sales")
        print("   🏆 top_products")  
        print("   🌍 country_sales")
        
        # Örnek veriler
        print("\n📋 Günlük Satış (Son 5 gün):")
        daily_sales.show(5)
        
        print("\n🏆 En Çok Satılan Ürünler (Top 5):")
        top_products.show(5)
        
        print("\n🌍 Ülke Bazlı Satış (Top 5):")
        country_sales.show(5)
        
    except Exception as e:
        print(f"❌ Gold aggregation hatası: {str(e)}")
        raise e
    finally:
        spark.stop()

if __name__ == "__main__":
    run_gold_aggregation()
