from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError
import sys


def create_spark_session():
    """Initialize Spark Session with MySQL connector"""
    return (
        SparkSession.builder.appName("mysql-etl")
        .master("local[*]")
        .config("spark.driver.extraClassPath", "mysql-connector-j-9.0.0.jar")
        .getOrCreate()
    )


def get_mysql_properties():
    """Return MySQL connection properties"""
    return {
        "url": "jdbc:mysql://localhost:3306/online_retail_data_warehouse?useSSL=false&allowPublicKeyRetrieval=true&autoReconnect=true",
        "driver": "com.mysql.cj.jdbc.Driver",
        "user": "admin",
        "password": "password",
    }


def truncate_tables(spark, mysql_props):
    """Truncate tables in correct order to handle foreign key constraints"""
    try:
        # Create connection for executing SQL commands
        from py4j.java_gateway import java_import

        java_import(spark._sc._jvm, "java.sql.DriverManager")
        conn = spark._sc._jvm.DriverManager.getConnection(
            mysql_props["url"], mysql_props["user"], mysql_props["password"]
        )

        # Disable foreign key checks
        cursor = conn.createStatement()
        cursor.execute("SET FOREIGN_KEY_CHECKS=0")

        # Truncate tables in correct order
        tables = [
            "fact_sales",
            "dim_customer",
            "dim_product",
            "dim_date",
            "dim_invoice",
        ]
        for table in tables:
            cursor.execute(f"TRUNCATE TABLE {table}")
            print(f"Truncated {table}")

        # Re-enable foreign key checks
        cursor.execute("SET FOREIGN_KEY_CHECKS=1")
        conn.close()
        print("All tables truncated successfully")

    except Exception as e:
        print(f"Error truncating tables: {str(e)}")
        raise


def load_to_mysql(df, table_name, mysql_props, mode="overwrite"):
    """Load DataFrame to MySQL with error handling"""
    try:
        df.write.format("jdbc").option("url", mysql_props["url"]).option(
            "driver", mysql_props["driver"]
        ).option("dbtable", table_name).option("user", mysql_props["user"]).option(
            "password", mysql_props["password"]
        ).mode(
            mode
        ).save()
        print(f"Successfully loaded {df.count()} records to {table_name}")
        return True
    except Exception as e:
        print(f"Error loading {table_name}: {str(e)}")
        return False


def etl_to_warehouse(gold_path):
    """Main ETL process to load data from gold layer to MySQL"""
    try:
        # Initialize Spark
        spark = create_spark_session()
        mysql_props = get_mysql_properties()

        # Read gold layer tables
        dim_customer = spark.read.parquet(f"{gold_path}/dim_customer")
        dim_product = spark.read.parquet(f"{gold_path}/dim_product")
        dim_date = spark.read.parquet(f"{gold_path}/dim_date")
        dim_invoice = spark.read.parquet(f"{gold_path}/dim_invoice")
        fact_sales = spark.read.parquet(f"{gold_path}/fact_sales")

        print("\n=== Starting ETL Process ===")

        # Truncate existing data
        truncate_tables(spark, mysql_props)

        # Load dimensions first (order matters for referential integrity)
        tables_to_load = [
            (dim_customer, "dim_customer", "Customer dimension"),
            (dim_product, "dim_product", "Product dimension"),
            (dim_date, "dim_date", "Date dimension"),
            (dim_invoice, "dim_invoice", "Invoice dimension"),
            (fact_sales, "fact_sales", "Facts table"),
        ]

        for df, table_name, description in tables_to_load:
            print(f"\nLoading {description}...")
            print(f"Records to load: {df.count()}")

            success = load_to_mysql(df, table_name, mysql_props)
            if not success:
                raise Exception(f"Failed to load {table_name}")

            # Verify load
            loaded_count = spark.read.jdbc(
                url=mysql_props["url"], table=table_name, properties=mysql_props
            ).count()
            print(f"Verified {loaded_count} records in {table_name}")

        print("\n=== ETL Process Completed Successfully ===")

    except Exception as e:
        print(f"\nETL Process Failed: {str(e)}")
        sys.exit(1)
    finally:
        spark.stop()


def main():
    # Define gold layer path
    GOLD_PATH = "hdfs://localhost:9000/retail_lake/gold"

    print("Starting data warehouse ETL process...")
    etl_to_warehouse(GOLD_PATH)


if __name__ == "__main__":
    main()
