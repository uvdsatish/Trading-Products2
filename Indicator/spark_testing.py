from pyspark.sql import SparkSession
import time
import os
import sys

# Set the Python path for the PySpark workers and driver
python_path = "C:/ProgramData/Anaconda3/python.exe"
os.environ["PYSPARK_PYTHON"] = python_path
os.environ["PYSPARK_DRIVER_PYTHON"] = python_path

# Initialize the SparkSession
spark = SparkSession.builder \
    .appName("PySpark Test") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.pyspark.python", python_path) \
    .config("spark.pyspark.driver.python", python_path) \
    .getOrCreate()

# Create a large dataset
data = [("John", 28), ("Anna", 23), ("Peter", 35)] * 10_000_000  # Simulate 30 million rows
columns = ["Name", "Age"]

# Timing the execution
start_time = time.time()

df = spark.createDataFrame(data, columns)

# Example operation: group by and aggregation
df_grouped = df.groupBy('Name').agg({'Age': 'avg'})
df_grouped.show()

end_time = time.time()
execution_time = end_time - start_time
print(f"PySpark execution time: {execution_time} seconds")

# Stop the Spark session
spark.stop()

sys.exit(0)
