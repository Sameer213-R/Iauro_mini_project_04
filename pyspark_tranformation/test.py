import os
import sys

# Python used by Spark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Hadoop home
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

print("Python:", sys.executable)
print("Python exists:", os.path.exists(sys.executable))
print("Hadoop:", os.environ["HADOOP_HOME"])
print("Hadoop exists:", os.path.exists(os.environ["HADOOP_HOME"]))

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("MyApp")
    .master("local[*]")
    .config("spark.hadoop.fs.permissions.umask-mode", "022")
    .getOrCreate()
)

print(
    "Native Hadoop loaded:",
    spark.sparkContext._jvm.org.apache.hadoop.util.NativeCodeLoader.isNativeCodeLoaded()
)

data = [
    (1, "Alice", "Pune"),
    (2, "Bob", "Mumbai"),
    (3, "Charlie", "Nashik")
]

df = spark.createDataFrame(
    data,
    ["id", "name", "city"]
)

df.show()

output_path = (
    r"E:\Iauro assignment\spark_cluster\spark_standalone_cluster"
    r"\collecting_s3_file\sink_file\test_output"
)

df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv(output_path)

print("WRITE SUCCESSFUL")

spark.stop()