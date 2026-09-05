from pyspark.sql import SparkSession
import os
import sys
from dotenv import load_dotenv

# Load .env
load_dotenv("collecting_s3_file/credentials/.env")

# Python configuration
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

# Hadoop configuration for Windows
os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"


# Create Spark Session
spark = (
    SparkSession.builder
    .appName("SparkS3Application")
    .master("local[*]")

    # Hadoop AWS connector
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4"
    )

    # S3A filesystem
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )

    # AWS credentials
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.getenv("AWS_ACCESS_KEY_ID")
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.getenv("AWS_SECRET_ACCESS_KEY")
    )

    # AWS region
    .config(
        "spark.hadoop.fs.s3a.endpoint",
        f"s3.{os.getenv('AWS_DEFAULT_REGION')}.amazonaws.com"
    )

    # Credential provider
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )

    .getOrCreate()
)

bucket_path = "s3a://aws-sink-data-346992621522-ap-south-1-an/Fact_table/"

# data_frame = spark.read.format("csv").option('header', True).option('inferSchema', True).load(bucket_path)

data_frame = spark.read.format("parquet").option('header', True).option('inferSchema', True).load(f"{bucket_path}/city=Delhi")
data_frame.show(5)
spark.stop()
print("record read successfully from s3 bucket and displayed in console")

