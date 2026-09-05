from pyspark.sql import SparkSession
from pyspark.sql.functions import col, initcap, lit, current_timestamp, regexp_replace, when
import time
import sys
import os
from dotenv import load_dotenv


# --------------------------------------------------
# Load environment variables
# --------------------------------------------------

load_dotenv("E:\Iauro assignment\spark_cluster\spark_standalone_cluster\collecting_s3_file\credentials\.env")


# --------------------------------------------------
# Ask for orders file name
# --------------------------------------------------

file_name = input("Enter the orders data file: ")


# --------------------------------------------------
# Python configuration for PySpark
# --------------------------------------------------

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


# --------------------------------------------------
# Hadoop configuration
# --------------------------------------------------

os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"


# --------------------------------------------------
# Create SparkSession
# --------------------------------------------------

spark = (
    SparkSession.builder
    .appName(os.getenv("app_name"))
    .master(os.getenv("master"))
    .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4"
        )
    .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem"
        )
    .config(
            "spark.hadoop.fs.s3a.access.key",
            os.getenv("AWS_ACCESS_KEY_ID")
        )
    .config(
            "spark.hadoop.fs.s3a.secret.key",
            os.getenv("AWS_SECRET_ACCESS_KEY")
        )
    .config(
            "spark.hadoop.fs.s3a.endpoint",
            f"s3.{os.getenv('AWS_DEFAULT_REGION')}.amazonaws.com"
        )
    .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        )
    .getOrCreate()
)


read_path_customer = "s3a://aws-s3-data-store/incoming/customers/customers_master.csv"
read_path_order = f"s3a://aws-s3-data-store/incoming/orders/{file_name}.csv"
# --------------------------------------------------
# Read CSV files
# --------------------------------------------------

df_customer = (
    spark.read
    .csv(
        read_path_customer,
        header=True,
        inferSchema=True
    )
)

df_order = (
    spark.read
    .csv(
        read_path_order,
        header=True,
        inferSchema=True
    )
)


# --------------------------------------------------
# Display input data
# --------------------------------------------------

print("\nCustomer Data:")
df_customer.show(5)

print("\nOrder Data:")
df_order.show(5)

# ------------------------------------------
# check that the email are in proper format
#-----------------------------------------
df_customer = df_customer.withColumn(
    "email_clean",
    when(
        col("email").rlike(r"gmail\.com$"),
        regexp_replace(col("email"), r"gmail\.com$", "@gmail.com")
    )
    .when(
        col("email").rlike(r"yahoo\.com$"),
        regexp_replace(col("email"), r"yahoo\.com$", "@yahoo.com")
    )
    .otherwise(col("email"))
)

#--------------------------------------------------
# droping the negative values from the order table and customer table
#--------------------------------------------------
df_order = df_order.filter(col("order_id") > 0).count()
df_customer = df_customer.filter(col("customer_id") > 0).count()

# del with the negative values from the customer_id column in the customer & orders table 
df_customer = df_customer.withColumn('customer_id', when(col('customer_id'<0),regexp_replace(col('customer_id'),"-","").otherwise(col('customer_id'))))
df_order = df_order.withColumn('order_id', when(col('order_id'<0),regexp_replace(col('order_id'),"-","").otherwise(col('order_id'))))

# --------------------------------------------------
# Join customer and order data
# --------------------------------------------------

df_order_customer = (
    df_customer
    .join(
        df_order,
        on="customer_id",
        how="inner"
    )
)


# --------------------------------------------------
# Create fact table
# --------------------------------------------------

df_fact = (
    df_order_customer
    .select(
        "order_id",
        "customer_id",
        "order_date",
        "status",
        "city"
    )
)


# --------------------------------------------------
# Add processed timestamp
# --------------------------------------------------

df_fact = (
    df_fact
    .withColumn(
        "processed_at",
        current_timestamp()
    )
    .withColumn(    
        'city', initcap('city')
)
)


# --------------------------------------------------
# Write fact table as Parquet
# Partition by city
# --------------------------------------------------

df_fact.write \
    .mode("overwrite") \
    .format("parquet") \
    .partitionBy("city") \
    .save("s3a://aws-sink-data-346992621522-ap-south-1-an/Fact_table/")


# --------------------------------------------------
# Create dimension customer table
# --------------------------------------------------

dim_customer = (
    df_customer
    .select(
        "customer_id",
        "customer_name",
        "email",
        "city"
    )
)


# --------------------------------------------------
# Write dimension customer as Parquet
# --------------------------------------------------

dim_customer.write \
    .mode("overwrite") \
    .format("parquet") \
    .save("s3a://aws-sink-data-346992621522-ap-south-1-an/Dim_customer/")


# --------------------------------------------------
# Print completion message
# --------------------------------------------------

print("\nALL DONE!")

# --------------------------------------------------
# Keep Spark application alive for Spark UI
# --------------------------------------------------

time.sleep(100)


# --------------------------------------------------
# Stop Spark
# --------------------------------------------------

spark.stop()