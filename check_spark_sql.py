# First, create a SparkSession if you haven't already
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("DataLakeHouse") \
    .getOrCreate()
    # .enableHiveSupport() \

# List all databases
spark.sql("SHOW DATABASES").show()

# Show current database
spark.sql("SELECT current_database()").show()

# Switch to a specific database
spark.sql("USE default")

# List all tables in current database
spark.sql("SHOW TABLES").show()

# Get detailed information about a specific table
# spark.sql("DESCRIBE TABLE table_name").show()

# Get extended table information including storage properties
# spark.sql("DESCRIBE EXTENDED table_name").show()

# Get formatted table information (more readable format)
# spark.sql("DESCRIBE FORMATTED table_name").show()