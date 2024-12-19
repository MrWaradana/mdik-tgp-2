from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    when,
    countDistinct,
    sum,
    avg,
    max,
    date_trunc,
    to_timestamp,
    date_format,
    year,
    month,
    dayofweek,
    dayofmonth,
    dayofyear,
    weekofyear,
    quarter,
    current_timestamp,
    to_date,
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
    """Clean data and create silver and gold layers matching the star schema"""
    # Read from bronze
    df = spark.read.parquet(bronze_path)

    # Clean data for silver layer
    cleaned_df = (
        df.dropna(subset=["InvoiceNo", "StockCode", "CustomerID"])
        .fillna({"Description": "NO DESCRIPTION", "Quantity": 0, "UnitPrice": 0})
        .dropDuplicates(["InvoiceNo", "StockCode"])
        .withColumn("InvoiceDate", to_timestamp("InvoiceDate", "dd/MM/yyyy HH:mm"))
        .withColumn("Quantity", col("Quantity").cast("integer"))
        .withColumn("UnitPrice", col("UnitPrice").cast("double"))
        .withColumn("TotalAmount", col("Quantity") * col("UnitPrice"))
        .filter(col("Quantity") > 0)
        .filter(col("UnitPrice") > 0)
    )

    # Save to silver
    cleaned_df.write.mode("overwrite").parquet(silver_path)
    print("Silver layer created successfully")

    # Create dimension tables for gold layer

    # 1. Customer Dimension
    dim_customer = (
        cleaned_df.select("CustomerID", "Country")
        .distinct()
        .withColumn("created_date", current_timestamp())
    )

    # 2. Product Dimension
    dim_product = (
        cleaned_df.select(
            col("StockCode").alias("stock_code"),
            col("Description").alias("description"),
            col("UnitPrice").alias("unit_price"),
        )
        .distinct()
        .withColumn("created_date", current_timestamp())
    )

    # 3. Date Dimension
    date_df = (
        cleaned_df.select(to_date("InvoiceDate").alias("full_date"))
        .distinct()
        .withColumn("date_id", date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("day_of_week", dayofweek("full_date"))
        .withColumn("day_name", date_format("full_date", "EEEE"))
        .withColumn("day_of_month", dayofmonth("full_date"))
        .withColumn("day_of_year", dayofyear("full_date"))
        .withColumn("week_of_year", weekofyear("full_date"))
        .withColumn("month", month("full_date"))
        .withColumn("month_name", date_format("full_date", "MMMM"))
        .withColumn("quarter", quarter("full_date"))
        .withColumn("year", year("full_date"))
        .withColumn("created_date", current_timestamp())
    )

    # 4. Invoice Dimension
    dim_invoice = (
        cleaned_df.select(col("InvoiceNo").alias("invoice_id"))
        .distinct()
        .withColumn(
            "invoice_type",
            when(col("invoice_id").startswith("C"), "Return").otherwise("Regular"),
        )
        .withColumn("created_date", current_timestamp())
    )

    # 5. Fact Sales
    fact_sales = cleaned_df.join(
        date_df.select("date_id", "full_date"),
        to_date(cleaned_df.InvoiceDate) == date_df.full_date,
    ).select(
        col("InvoiceNo").alias("invoice_id"),
        col("CustomerID").alias("customer_id"),
        col("StockCode").alias("stock_code"),
        col("date_id"),
        col("Quantity").alias("quantity"),
        col("UnitPrice").alias("unit_price"),
        col("TotalAmount").alias("total_amount"),
        current_timestamp().alias("created_date"),
    )

    # Save gold tables
    dim_customer.write.mode("overwrite").parquet(f"{gold_path}/dim_customer")
    dim_product.write.mode("overwrite").parquet(f"{gold_path}/dim_product")
    date_df.write.mode("overwrite").parquet(f"{gold_path}/dim_date")
    dim_invoice.write.mode("overwrite").parquet(f"{gold_path}/dim_invoice")
    fact_sales.write.mode("overwrite").parquet(f"{gold_path}/fact_sales")

    print("Gold layer created successfully")

    return {
        "silver": cleaned_df,
        "dim_customer": dim_customer,
        "dim_product": dim_product,
        "dim_date": date_df,
        "dim_invoice": dim_invoice,
        "fact_sales": fact_sales,
    }


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

        # Ingest raw data to bronze layer
        ingest_data(spark, bronze_path=BRONZE_PATH)

        # Process data to silver layer and gold layer
        result_dfs = process_data(spark, BRONZE_PATH, SILVER_PATH, GOLD_PATH)

        # Print sample of each DataFrame
        print("\n=== Silver Layer Sample ===")
        result_dfs["silver"].show(1, truncate=False)

        print("\n=== Dimension Tables Samples ===")
        print("Customer Dimension:")
        result_dfs["dim_customer"].show(1, truncate=False)

        print("\nProduct Dimension:")
        result_dfs["dim_product"].show(1, truncate=False)

        print("\nDate Dimension:")
        result_dfs["dim_date"].show(1, truncate=False)

        print("\nInvoice Dimension:")
        result_dfs["dim_invoice"].show(1, truncate=False)

        print("\n=== Fact Table Sample ===")
        result_dfs["fact_sales"].show(1, truncate=False)

        print("\n=== Data Quality Summary ===")
        for name, df in result_dfs.items():
            print(f"\n{name} DataFrame:")
            print(f"Number of records: {df.count()}")
            # print("Schema:")
            # df.printSchema()

        print("Pipeline completed successfully!")

    except Exception as e:
        print(f"Pipeline failed: {str(e)}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
