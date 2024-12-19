from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    to_timestamp,
    col,
    count,
    when,
    countDistinct,
    sum,
    avg,
    max,
    date_trunc,
)


def create_spark_session():
    """Initialize Spark Session"""
    return SparkSession.builder.appName("Online Retail Lake").getOrCreate()


def ingest_data(
    spark,
    input_path="Online_Retail.csv",
    bronze_path="hdfs://localhost:9000/retail_lake/bronze/",
):
    """Load data and save to bronze layer"""
    # Read CSV
    df = spark.read.csv(input_path, header=True, inferSchema=True)

    # Basic data profiling
    print(f"Total records: {df.count()}")
    print(f"Columns: {df.columns}")
    df.select([count(when(col(c).isNull(), c)).alias(c) for c in df.columns]).show()

    # Save to bronze
    df.write.mode("overwrite").parquet(bronze_path)
    print("Bronze layer created successfully")

    return df


def process_data(spark, bronze_path, silver_path, gold_path):
    """Clean data and create silver and gold layers"""
    # Read from bronze
    df = spark.read.parquet(bronze_path)

    # Clean data
    cleaned_df = (
        df.dropna(subset=["InvoiceNo", "StockCode", "CustomerID"])
        .fillna({"Description": "NO DESCRIPTION", "Quantity": 0, "UnitPrice": 0})
        .dropDuplicates(["InvoiceNo", "StockCode"])
        .withColumn("InvoiceDate", to_timestamp("InvoiceDate", "dd-MM-yyyy HH:mm"))
        .withColumn("Quantity", col("Quantity").cast("integer"))
        .withColumn("UnitPrice", col("UnitPrice").cast("double"))
        .withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))
        .filter(col("Quantity") > 0)
        .filter(col("UnitPrice") > 0)
    )

    # Save to silver
    cleaned_df.write.mode("overwrite").parquet(silver_path)
    print("Silver layer created successfully")

    # Create gold tables
    sales_summary = cleaned_df.groupBy(
        "Country", date_trunc("month", "InvoiceDate").alias("Month")
    ).agg(
        countDistinct("InvoiceNo").alias("TotalOrders"),
        sum("TotalAmount").alias("Revenue"),
        avg("TotalAmount").alias("AverageOrderValue"),
    )

    customer_metrics = cleaned_df.groupBy("CustomerID", "Country").agg(
        count("InvoiceNo").alias("PurchaseFrequency"),
        sum("TotalAmount").alias("TotalSpend"),
        avg("TotalAmount").alias("AvgOrderValue"),
        max("InvoiceDate").alias("LastPurchaseDate"),
    )

    # Save gold tables
    sales_summary.write.mode("overwrite").parquet(f"{gold_path}/sales_summary")
    customer_metrics.write.mode("overwrite").parquet(f"{gold_path}/customer_metrics")
    print("Gold layer created successfully")

    return cleaned_df, sales_summary, customer_metrics


def create_views_and_exports(spark, sales_df, customer_df, export_path="/exports/bi/"):
    """Create views and export data for BI tools"""
    # Create database if not exists
    spark.sql("CREATE DATABASE IF NOT EXISTS retail_views")

    # Drop existing tables if they exist
    spark.sql("DROP TABLE IF EXISTS retail_views.sales_summary_table")
    spark.sql("DROP TABLE IF EXISTS retail_views.customer_metrics_table")
    spark.sql("DROP VIEW IF EXISTS retail_views.customer_segments")

    # Create permanent tables instead of temporary views
    sales_df.write.mode("overwrite").saveAsTable("sales_summary_table_view")
    customer_df.write.mode("overwrite").saveAsTable("customer_metrics_table_view")

    # Create customer segments view
    spark.sql(
        """
        CREATE OR REPLACE VIEW retail_views.customer_segments_table_view AS
        SELECT 
            CustomerID,
            TotalSpend,
            CASE 
                WHEN TotalSpend > 1000 THEN 'High Value'
                WHEN TotalSpend > 500 THEN 'Medium Value'
                ELSE 'Low Value'
            END as CustomerSegment
        FROM customer_metrics_table_view
    """
    )

    # Export for BI
    # sales_df.write.mode("overwrite").csv(f"{export_path}/sales_summary_table_view")
    # customer_df.write.mode("overwrite").csv(
    #     f"{export_path}/customer_metrics_table_view"
    # )
    print("Views created and data exported successfully")


def clear_table(spark):
    # Get list of all databases
    databases = spark.sql("SHOW DATABASES").collect()

    # Iterate through databases and drop them
    # for db in databases:
    #     db_name = db["databaseName"]
    #     if db_name != "default":  # Optionally keep the default database
    #         spark.sql(f"DROP DATABASE IF EXISTS {db_name} CASCADE")

    # For the default database, drop all tables
    tables = spark.sql("SHOW TABLES").collect()
    print(tables)
    for table in tables:
        table_name = table["tableName"]
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")

    # Reset the default database (optional)
    # spark.sql("DROP DATABASE IF EXISTS default CASCADE")
    # spark.sql("CREATE DATABASE IF NOT EXISTS default")
    # spark.sql("USE default")

    print("All databases and tables have been reset.")


def main():
    """Main pipeline execution"""
    # Define paths
    BRONZE_PATH = "hdfs://localhost:9000/retail_lake/bronze/"
    SILVER_PATH = "hdfs://localhost:9000/retail_lake/silver/"
    GOLD_PATH = "hdfs://localhost:9000/retail_lake/gold/"

    try:
        # Initialize Spark
        spark = create_spark_session()

        # Execute pipeline
        print("Starting data pipeline...")
        # clear_table(spark)

        # Ingest data
        ingest_data(spark, bronze_path=BRONZE_PATH)

        # Process data
        cleaned_df, sales_summary_table_view, customer_metrics_table_view = (
            process_data(spark, BRONZE_PATH, SILVER_PATH, GOLD_PATH)
        )

        # Create views and exports
        # create_views_and_exports(
        #     spark, sales_summary_table_view, customer_metrics_table_view
        # )

        print("Pipeline completed successfully!")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
