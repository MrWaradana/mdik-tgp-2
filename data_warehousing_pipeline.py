from pyspark.sql import SparkSession
from py4j.protocol import Py4JJavaError

try:
    # Create Spark session if not exists
    if 'spark' not in locals():
        spark = SparkSession \
            .builder \
            .appName('mysql-connection') \
            .master('local[*]') \
            .config("spark.driver.extraClassPath", "mysql-connector-j-9.0.0.jar") \
            .getOrCreate()

    # Connect to MySQL and load data
    df = (
        spark.read.format("jdbc")
        .option("url", "jdbc:mysql://localhost:3306/online_retail_data_warehouse?useSSL=false&allowPublicKeyRetrieval=true&autoReconnect=true")  # Added port 3306
        .option("driver", "com.mysql.cj.jdbc.Driver")  # Updated driver class
        .option("dbtable", "dim_customer")
        .option("user", "admin")
        .option("password", "password")
        .load()
    )

    # Verify connection by showing DataFrame schema and sample data
    print("Table Schema:")
    df.printSchema()
    
    print("\nSample Data:")
    df.show(5)
    
    print("\nTotal Records:", df.count())

except Py4JJavaError as e:
    print("Java Exception Error:")
    print(str(e))
except Exception as e:
    print("Python Exception Error:")
    print(str(e))