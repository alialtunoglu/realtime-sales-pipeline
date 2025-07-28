"""
Unit Tests for Bronze ETL Operations
Tests data extraction, transformation, and loading logic
"""
import pytest
import tempfile
import os
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Add project root to path
import sys
from pathlib import Path
project_root = Path(__file__).parent.parent.parent
sys.path.append(str(project_root))

from src.etl.bronze_ingestion import BronzeETL


class TestBronzeETL:
    """Test suite for Bronze ETL operations"""
    
    @pytest.fixture(scope="class")
    def spark(self):
        """Create Spark session for testing"""
        return SparkSession.builder \
            .appName("test_bronze_etl") \
            .master("local[1]") \
            .config("spark.sql.warehouse.dir", tempfile.mkdtemp()) \
            .getOrCreate()
    
    @pytest.fixture
    def bronze_etl(self, spark):
        """Create BronzeETL instance with mocked dependencies"""
        with patch('src.etl.bronze_ingestion.get_spark', return_value=spark):
            with patch('src.etl.bronze_ingestion.config') as mock_config:
                mock_config.storage.get_absolute_path.return_value = "/test/path"
                mock_config.storage.bronze_path = "test_bronze"
                return BronzeETL()
    
    @pytest.fixture
    def sample_csv_data(self, spark):
        """Create sample CSV data for testing"""
        schema = StructType([
            StructField("InvoiceNo", StringType(), True),
            StructField("StockCode", StringType(), True),
            StructField("Description", StringType(), True),
            StructField("Quantity", IntegerType(), True),
            StructField("InvoiceDate", StringType(), True),
            StructField("UnitPrice", DoubleType(), True),
            StructField("CustomerID", StringType(), True),
            StructField("Country", StringType(), True)
        ])
        
        data = [
            ("536365", "85123A", "WHITE HANGING HEART", 6, "2010-12-01 08:26:00", 2.55, "17850", "UK"),
            ("536366", "22423", "REGENCY CAKESTAND", 12, "2010-12-01 08:26:00", 3.39, "17850", "UK"),
            ("536367", "84879", "ASSORTED COLOUR BIRD", 32, "2010-12-01 08:26:00", 2.75, "13047", "UK"),
            ("536368", "22720", "SET OF 3 CAKE TINS", 6, "2010-12-01 08:26:00", 4.25, "13047", "UK"),
            ("536369", "21730", "GLASS STAR FROSTED", 6, "2010-12-01 08:26:00", 4.25, "13047", "UK")
        ]
        
        return spark.createDataFrame(data, schema)
    
    def test_extract_from_csv_success(self, bronze_etl, sample_csv_data):
        """Test successful CSV extraction"""
        # Create temporary CSV file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False) as f:
            # Write CSV content
            f.write("InvoiceNo,StockCode,Description,Quantity,InvoiceDate,UnitPrice,CustomerID,Country\n")
            f.write("536365,85123A,WHITE HANGING HEART,6,2010-12-01 08:26:00,2.55,17850,UK\n")
            f.write("536366,22423,REGENCY CAKESTAND,12,2010-12-01 08:26:00,3.39,17850,UK\n")
            temp_file = f.name
        
        try:
            # Mock the config to return our temp file path
            with patch.object(bronze_etl.config.storage, 'get_absolute_path', return_value=temp_file):
                df = bronze_etl.extract_from_csv("test.csv")
                
                # Assertions
                assert df is not None
                assert df.count() == 2
                assert "InvoiceNo" in df.columns
                assert "StockCode" in df.columns
                
        finally:
            os.unlink(temp_file)
    
    def test_extract_from_csv_file_not_found(self, bronze_etl):
        """Test CSV extraction with missing file"""
        with patch.object(bronze_etl.config.storage, 'get_absolute_path', return_value="/nonexistent/file.csv"):
            with pytest.raises(Exception):
                bronze_etl.extract_from_csv("nonexistent.csv")
    
    def test_transform_bronze(self, bronze_etl, sample_csv_data):
        """Test Bronze layer transformations"""
        df_bronze = bronze_etl.transform_bronze(sample_csv_data)
        
        # Check that new columns are added
        assert "ingestion_timestamp" in df_bronze.columns
        
        # Check that original data is preserved
        assert df_bronze.count() == sample_csv_data.count()
        
        # Check that all original columns are present
        for col in sample_csv_data.columns:
            assert col in df_bronze.columns
    
    def test_load_to_bronze_success(self, bronze_etl, sample_csv_data):
        """Test successful Bronze table loading"""
        with tempfile.TemporaryDirectory() as temp_dir:
            # Mock the storage path
            bronze_path = os.path.join(temp_dir, "test_table")
            
            with patch.object(bronze_etl.config.storage, 'get_absolute_path', return_value=bronze_path):
                # This should not raise an exception
                bronze_etl.load_to_bronze(sample_csv_data, "test_table")
                
                # Verify that Delta table was created
                assert os.path.exists(bronze_path)
                assert os.path.exists(os.path.join(bronze_path, "_delta_log"))
    
    def test_data_quality_metrics_collection(self, bronze_etl, sample_csv_data):
        """Test data quality metrics collection"""
        # Add some null values for testing
        from pyspark.sql.functions import when, lit
        
        df_with_nulls = sample_csv_data.withColumn(
            "CustomerID", 
            when(sample_csv_data.CustomerID == "17850", None).otherwise(sample_csv_data.CustomerID)
        )
        
        # Mock logger to capture metrics
        with patch.object(bronze_etl, 'logger') as mock_logger:
            bronze_etl._log_data_quality_metrics(df_with_nulls, "test_table")
            
            # Verify that data quality logging was called
            mock_logger.log_data_quality.assert_called_once()
            
            # Get the call arguments
            call_args = mock_logger.log_data_quality.call_args
            table_name = call_args[0][0]
            metrics = call_args[0][1]
            
            assert table_name == "test_table"
            assert "total_rows" in metrics
            assert "null_counts" in metrics
            assert "completeness_score" in metrics
            assert metrics["total_rows"] == 5
    
    @patch('time.time')
    def test_run_bronze_ingestion_success(self, mock_time, bronze_etl):
        """Test complete Bronze ingestion pipeline"""
        # Mock time for duration calculation
        mock_time.side_effect = [0, 10]  # Start time, end time
        
        # Mock the individual methods
        with patch.object(bronze_etl, 'extract_from_csv') as mock_extract, \
             patch.object(bronze_etl, 'transform_bronze') as mock_transform, \
             patch.object(bronze_etl, 'load_to_bronze') as mock_load, \
             patch.object(bronze_etl, 'logger') as mock_logger:
            
            # Setup mock returns
            mock_df = Mock()
            mock_df.count.return_value = 100
            mock_extract.return_value = mock_df
            mock_transform.return_value = mock_df
            
            # Run the pipeline
            bronze_etl.run_bronze_ingestion("test.csv", "test_table")
            
            # Verify all methods were called
            mock_extract.assert_called_once_with("test.csv")
            mock_transform.assert_called_once_with(mock_df)
            mock_load.assert_called_once_with(mock_df, "test_table")
            
            # Verify logging
            mock_logger.log_pipeline_start.assert_called_once()
            mock_logger.log_pipeline_end.assert_called_once()
            
            # Check pipeline end call
            end_call_args = mock_logger.log_pipeline_end.call_args[1]
            assert end_call_args["status"] == "success"
            assert end_call_args["duration"] == 10
    
    @patch('time.time')
    def test_run_bronze_ingestion_failure(self, mock_time, bronze_etl):
        """Test Bronze ingestion pipeline with failure"""
        # Mock time for duration calculation
        mock_time.side_effect = [0, 5]  # Start time, end time
        
        # Mock methods with exception
        with patch.object(bronze_etl, 'extract_from_csv', side_effect=Exception("Test error")) as mock_extract, \
             patch.object(bronze_etl, 'logger') as mock_logger:
            
            # Run the pipeline and expect exception
            with pytest.raises(Exception, match="Test error"):
                bronze_etl.run_bronze_ingestion("test.csv", "test_table")
            
            # Verify logging
            mock_logger.log_pipeline_start.assert_called_once()
            mock_logger.log_pipeline_end.assert_called_once()
            
            # Check pipeline end call
            end_call_args = mock_logger.log_pipeline_end.call_args[1]
            assert end_call_args["status"] == "failed"
            assert "error" in end_call_args
