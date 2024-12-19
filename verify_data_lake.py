from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when
import sys

def create_spark_session():
    """Initialize Spark Session"""
    return SparkSession.builder \
        .appName("Data Lake Verification") \
        .getOrCreate()

def verify_layer_existence(spark, path):
    """Verify if a layer exists and is readable"""
    try:
        df = spark.read.parquet(path)
        return True, df
    except Exception as e:
        return False, str(e)

def check_null_counts(df):
    """Check null values based on column data type"""
    null_counts = df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in df.columns
    ])
    return null_counts

def check_data_quality(df, layer_name):
    """Check data quality metrics"""
    print(f"\n=== {layer_name} Data Quality Report ===")
    
    # Record count
    total_records = df.count()
    print(f"Total Records: {total_records}")
    
    # Schema information
    print("\nSchema:")
    df.printSchema()
    
    # Null values check
    print("\nNull Values Count:")
    null_counts = check_null_counts(df)
    null_counts.show()
    
    # Sample data
    print("\nSample Data:")
    df.show(5, truncate=False)
    
    return total_records > 0

def verify_gold_tables(spark, gold_path):
    """Verify specific gold layer tables"""
    gold_tables = ['sales_summary', 'customer_metrics']
    results = {}
    
    for table in gold_tables:
        table_path = f"{gold_path}/{table}"
        exists, result = verify_layer_existence(spark, table_path)
        if exists:
            print(f"\n=== Verifying Gold Table: {table} ===")
            is_valid = check_data_quality(result, f"Gold - {table}")
            results[table] = "Valid" if is_valid else "Invalid (Empty)"
        else:
            results[table] = f"Not Found: {result}"
    
    return results

def check_column_requirements(df, layer_name):
    """Check if required columns are present"""
    required_columns = {
        'silver': ['InvoiceNo', 'StockCode', 'Description', 'Quantity', 
                  'InvoiceDate', 'UnitPrice', 'CustomerID', 'Country'],
        'gold_sales_summary': ['Country', 'Month', 'TotalOrders', 'Revenue', 'AverageOrderValue'],
        'gold_customer_metrics': ['CustomerID', 'Country', 'PurchaseFrequency', 
                                'TotalSpend', 'AvgOrderValue', 'LastPurchaseDate']
    }
    
    if layer_name in required_columns:
        missing_columns = [col for col in required_columns[layer_name] 
                         if col not in df.columns]
        if missing_columns:
            print(f"\nWarning: Missing required columns in {layer_name}:")
            print(missing_columns)
            return False
    return True

def main():
    """Main verification process"""
    # Define paths
    SILVER_PATH = "hdfs://localhost:9000/retail_lake/silver"
    GOLD_PATH = "hdfs://localhost:9000/retail_lake/gold"
    
    try:
        spark = create_spark_session()
        print("Starting data lake verification...\n")
        
        # Verify Silver Layer
        print("=== Verifying Silver Layer ===")
        silver_exists, silver_result = verify_layer_existence(spark, SILVER_PATH)
        if silver_exists:
            silver_valid = check_data_quality(silver_result, "Silver")
            silver_columns_valid = check_column_requirements(silver_result, "silver")
            print(f"\nSilver Layer Status: {'Valid' if silver_valid and silver_columns_valid else 'Invalid'}")
        else:
            print(f"Silver Layer Error: {silver_result}")
            sys.exit(1)
            
        # Verify Gold Layer Tables
        print("\n=== Verifying Gold Layer ===")
        gold_results = verify_gold_tables(spark, GOLD_PATH)
        
        # Print Summary
        print("\n=== Verification Summary ===")
        print(f"Silver Layer: {'Valid' if silver_valid and silver_columns_valid else 'Invalid'}")
        print("\nGold Layer Tables:")
        for table, status in gold_results.items():
            print(f"- {table}: {status}")
        
        # Final Assessment
        all_valid = (silver_valid and silver_columns_valid and 
                    all(status == "Valid" for status in gold_results.values()))
        print("\nFinal Assessment:")
        if all_valid:
            print("✅ All layers are valid and ready for data warehouse loading")
        else:
            print("❌ Some layers have issues and need attention")
            sys.exit(1)
            
    except Exception as e:
        print(f"Verification failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()

if __name__ == "__main__":
    main()