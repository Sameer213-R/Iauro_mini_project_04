import boto3
from dotenv import load_dotenv
load_dotenv("collecting_s3_file/credentials/.env")
import os 

s3_client = boto3.client('s3')

# downlaoding the file form s3 bucker 

# bucket = s3_client.download_file(os.getenv("bucker_name"), os.getenv("object_name"), os.getenv("local_file_path"))
# print("File downloaded successfully from S3 bucket to local directory")

uploading_fact = s3_client.upload_file(os.getenv("fact_path"), os.getenv("fact_bucket_name"), os.getenv("fact_object_name"))
print("Fact table uploaded successfully to S3 bucket")
