# PySpark S3 ETL Pipeline

## 📌 Project Overview

This project implements an **ETL (Extract, Transform, Load) pipeline using PySpark** to process raw data stored in an **Amazon S3 bucket**.

The pipeline extracts raw data from S3, applies data cleaning and transformation rules, performs data-quality validations, creates a dimensional data model containing **Fact and Dimension tables**, and finally stores the transformed data back into S3 in **Parquet format**.

The Fact table is partitioned based on **City** to improve query performance and make data retrieval more efficient.

---

## 🏗️ ETL Architecture

```text
                 ┌─────────────────┐
                 │   Amazon S3     │
                 │   Raw Data      │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │     Extract     │
                 │   PySpark Read  │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │   Transform     │
                 │                 │
                 │ • Cleaning      │
                 │ • Formatting    │
                 │ • Null Handling │
                 │ • Data Types    │
                 │ • Business Rules│
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ Data Quality     │
                 │    Testing      │
                 │                 │
                 │ • Negative Values│
                 │ • Email Format  │
                 │ • Null Checks   │
                 │ • Duplicates    │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │ Data Modeling   │
                 │                 │
                 │ Dimension Tables│
                 │ Fact Table      │
                 └────────┬────────┘
                          │
                          ▼
                 ┌─────────────────┐
                 │   Amazon S3     │
                 │ Parquet Output  │
                 │                 │
                 │ Fact Partitioned│
                 │    by City      │
                 └─────────────────┘
```

---

# 🎯 Project Objectives

The main objectives of this project are:

* Read raw data from Amazon S3 using PySpark.
* Perform data cleaning and transformation.
* Handle missing and invalid values.
* Validate email formats.
* Identify negative or invalid numeric values.
* Remove or handle duplicate records.
* Apply business transformation rules.
* Create a dimensional data model.
* Create Fact and Dimension tables.
* Partition the Fact table based on City.
* Store processed data in Amazon S3.
* Use Parquet as the storage format.
* Perform data-quality testing before loading the final data.

---

# 🔄 ETL Workflow

The pipeline follows these major steps:

```text
1. Extract
     ↓
2. Data Profiling
     ↓
3. Data Cleaning
     ↓
4. Data Transformation
     ↓
5. Data Quality Testing
     ↓
6. Create Dimension Tables
     ↓
7. Create Fact Table
     ↓
8. Partition Fact Table
     ↓
9. Write Parquet Files
     ↓
10. Store Output in S3
```

---

# 1. Extract

The raw data is stored in an Amazon S3 bucket.

PySpark reads the data from S3 using the appropriate S3 path.

Example:

```python
df = spark.read.csv(
    "s3://bucket-name/raw-data/",
    header=True,
    inferSchema=True
)
```

Depending on the source data, the pipeline can support formats such as:

* CSV
* JSON
* Parquet

The raw data is loaded into a PySpark DataFrame for further processing.

---

# 2. Data Profiling

Before applying transformations, the dataset is analyzed to understand its structure and quality.

Typical profiling operations include:

```python
df.printSchema()

df.show()

df.count()

df.describe().show()
```

We also check:

* Number of records
* Column names
* Data types
* Null values
* Duplicate records
* Invalid values
* Minimum and maximum values
* Unique values

---

# 3. Data Cleaning

The raw dataset may contain incorrect or inconsistent data.

The ETL pipeline performs several cleaning operations.

### Null Value Handling

Example:

```python
df = df.fillna({
    "city": "Unknown",
    "email": "unknown@example.com"
})
```

Depending on the business requirement, records containing critical null values can also be rejected.

---

# 4. Email Validation

Email addresses are validated using a regular expression.

Example:

```python
from pyspark.sql.functions import col

df = df.withColumn(
    "email_valid",
    col("email").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
)
```

This creates a validation column:

```text
email                  email_valid
-----------------------------------
abc@gmail.com          true
abc@yahoo.com          true
abcgmail.com           false
abc@                    false
```

Invalid email records can be separated into a rejected-data dataset.

---

# 5. Numeric Data Validation

The pipeline checks numeric columns for invalid negative values.

For example, if `amount` should never be negative:

```python
df = df.withColumn(
    "amount_valid",
    col("amount") >= 0
)
```

Records can then be separated:

```python
valid_df = df.filter(col("amount_valid") == True)

invalid_df = df.filter(col("amount_valid") == False)
```

This allows invalid records to be identified without loading them into the final Fact table.

---

# 6. Duplicate Record Handling

Duplicate records are checked and removed based on the appropriate business key.

Example:

```python
df = df.dropDuplicates(["customer_id"])
```

For transaction data:

```python
df = df.dropDuplicates(["transaction_id"])
```

The key used for duplicate detection depends on the business requirements.

---

# 7. Data Transformation

After cleaning and validation, business transformations are applied.

Typical transformations include:

* Column renaming
* Data type conversion
* String formatting
* Date formatting
* Derived columns
* Conditional transformations
* Standardizing values
* Calculated metrics

Example:

```python
from pyspark.sql.functions import col, when

df = df.withColumn(
    "customer_type",
    when(col("amount") >= 10000, "Premium")
    .otherwise("Regular")
)
```

---

# 8. Dimensional Data Model

The transformed data is organized into a dimensional model consisting of:

```text
                 ┌─────────────────┐
                 │   dim_customer  │
                 └────────┬────────┘
                          │
                          │
┌─────────────────┐      ▼      ┌─────────────────┐
│   dim_city      │──── FACT ───│   dim_date      │
└─────────────────┘      ▲      └─────────────────┘
                          │
                          │
                 ┌────────┴────────┐
                 │  dim_product   │
                 └────────────────┘
```

The exact dimensions depend on the source dataset and business requirements.

---

# 9. Dimension Tables

Dimension tables contain descriptive information about business entities.

Possible dimension tables include:

### dim_customer

Contains customer-related information.

Example columns:

```text
customer_key
customer_id
customer_name
email
city
customer_type
```

### dim_city

Contains city-related information.

Example:

```text
city_key
city
state
country
```

### dim_product

Contains product-related information.

Example:

```text
product_key
product_id
product_name
category
```

### dim_date

Contains date-related information.

Example:

```text
date_key
date
day
month
year
quarter
```

---

# 10. Fact Table

The Fact table contains measurable business events or transactions.

Example:

```text
fact_sales

transaction_id
customer_key
product_key
city_key
date_key
quantity
amount
discount
total_amount
```

The Fact table connects the different Dimension tables through their keys.

---

# 11. Fact Table Partitioning

The Fact table is partitioned by **City**.

Example:

```python
fact_df.write \
    .mode("overwrite") \
    .partitionBy("city") \
    .parquet(
        "s3://bucket-name/processed/fact_sales/"
    )
```

This produces a directory structure similar to:

```text
fact_sales/
│
├── city=Mumbai/
│   ├── part-00000.parquet
│   └── part-00001.parquet
│
├── city=Pune/
│   ├── part-00000.parquet
│   └── part-00001.parquet
│
├── city=Nashik/
│   └── part-00000.parquet
│
└── city=Delhi/
    └── part-00000.parquet
```

### Why partition by City?

Partitioning allows Spark to avoid scanning unnecessary data when a query filters by City.

For example:

```sql
SELECT *
FROM fact_sales
WHERE city = 'Pune';
```

Spark can perform **partition pruning** and read only the relevant partition instead of scanning the complete dataset.

---

# 12. Parquet Storage

The final processed data is stored in **Parquet format**.

Parquet is used because it provides:

* Columnar storage
* Compression
* Efficient analytical queries
* Schema preservation
* Predicate pushdown
* Better performance for Spark workloads

Example:

```python
df.write \
    .mode("overwrite") \
    .parquet(
        "s3://bucket-name/processed/"
    )
```

---

# 13. Data Quality Framework

Data-quality checks are performed before the final data is written.

The pipeline validates:

| Test                 | Description                              |
| -------------------- | ---------------------------------------- |
| Null Check           | Checks required columns for NULL values  |
| Email Check          | Validates email format                   |
| Negative Value Check | Detects invalid negative numeric values  |
| Duplicate Check      | Detects duplicate business records       |
| Data Type Check      | Verifies expected data types             |
| Range Check          | Validates values against expected ranges |
| Required Field Check | Ensures mandatory fields are populated   |

---

# 14. Valid and Rejected Data

The pipeline separates valid and invalid records.

```text
                 Transformed Data
                       │
                       ▼
                Data Quality Tests
                       │
                ┌──────┴──────┐
                │             │
             VALID          INVALID
                │             │
                ▼             ▼
          Fact/Dimension    Rejected
             Tables           Data
                │             │
                ▼             ▼
             Parquet        Parquet
                │             │
                └──────┬──────┘
                       ▼
                       S3
```

Rejected records can be stored separately for further investigation.

Example:

```text
s3://bucket-name/
│
├── processed/
│   ├── fact_sales/
│   ├── dim_customer/
│   ├── dim_product/
│   └── dim_city/
│
└── rejected/
    ├── invalid_email/
    ├── negative_amount/
    └── duplicate_records/
```

---

# 📁 Project Structure

```text
pyspark-etl-project/
│
├── src/
│   │
│   ├── main.py
│   │
│   ├── config/
│   │   └── config.py
│   │
│   ├── extraction/
│   │   └── s3_reader.py
│   │
│   ├── transformation/
│   │   ├── cleaning.py
│   │   ├── transformation.py
│   │   └── validation.py
│   │
│   ├── dimensions/
│   │   ├── customer.py
│   │   ├── product.py
│   │   ├── city.py
│   │   └── date.py
│   │
│   ├── fact/
│   │   └── fact_sales.py
│   │
│   └── utils/
│       └── spark_session.py
│
├── tests/
│   ├── test_email.py
│   ├── test_negative_values.py
│   ├── test_duplicates.py
│   └── test_transformations.py
│
├── data/
│   └── sample/
│
├── requirements.txt
│
├── Dockerfile
│
├── docker-compose.yml
│
└── README.md
```

---

# ⚙️ Technologies Used

* **Python**
* **PySpark**
* **Apache Spark**
* **Amazon S3**
* **Parquet**
* **Docker**
* **SQL**
* **PyTest**

---

# 🔐 Configuration

S3 configuration should not be hardcoded inside the application.

Example configuration:

```text
AWS_REGION
S3_RAW_PATH
S3_PROCESSED_PATH
S3_REJECTED_PATH
```

Credentials should be provided through the appropriate AWS authentication mechanism rather than committed to source control.

---

# ▶️ Running the Project

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run the PySpark Application

```bash
spark-submit src/main.py
```

---

# 🐳 Running with Docker

Build the image:

```bash
docker build -t pyspark-etl .
```

Run the application:

```bash
docker run pyspark-etl
```

For a multi-container environment:

```bash
docker compose up
```

---

# 🧪 Testing

Tests can be executed using:

```bash
pytest tests/
```

Example validation test:

```python
def test_email_format():
    assert valid_email("test@gmail.com") is True
```

Negative-value test:

```python
def test_negative_amount():
    assert validate_amount(-100) is False
```

---

# 📊 Expected Output

After successful execution, the S3 processed location will contain:

```text
processed/
│
├── fact_sales/
│   ├── city=Mumbai/
│   ├── city=Pune/
│   ├── city=Nashik/
│   └── city=Delhi/
│
├── dim_customer/
│
├── dim_product/
│
├── dim_city/
│
└── dim_date/
```

All final datasets are stored as **Parquet files**.

---

# 🚀 Future Improvements

Possible improvements to the project include:

* Incremental data processing
* S3-based checkpointing
* AWS Glue Catalog integration
* Apache Airflow orchestration
* Data lineage
* Logging and monitoring
* Retry mechanisms
* Schema evolution
* Slowly Changing Dimensions (SCD)
* Delta Lake/Iceberg table support
* Automated data-quality reporting

---

# 👨‍💻 Learning Outcomes

This project demonstrates practical Data Engineering concepts including:

* PySpark DataFrame operations
* ETL pipeline development
* S3 data ingestion
* Data cleaning
* Regular expressions and email validation
* Data-quality testing
* Fact and Dimension modeling
* Data partitioning
* Partition pruning
* Parquet storage
* Spark transformations
* Dockerized Spark applications
* Cloud-based data processing

---

# 📌 Summary

This project demonstrates an end-to-end **PySpark ETL pipeline** that extracts raw data from Amazon S3, cleans and transforms the data, performs data-quality validation, creates Fact and Dimension tables, partitions the Fact table by City, and stores the final datasets in Amazon S3 using Parquet format.

The project provides a foundation for building scalable and production-oriented data engineering pipelines using **PySpark, Spark, AWS S3, and dimensional data modeling**.
