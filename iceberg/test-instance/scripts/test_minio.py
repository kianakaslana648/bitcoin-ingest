import boto3
from botocore.client import Config

s3 = boto3.client(
    's3',
    endpoint_url='http://minio:9000',
    aws_access_key_id='minioadmin',
    aws_secret_access_key='minioadmin',
    config=Config(signature_version='s3v4')
)

print("testing Minio connection")
print("buckets info:")
print(s3.list_buckets())

bucket_name = "test-bucket"
buckets = [b['Name'] for b in s3.list_buckets()['Buckets']]
if bucket_name not in buckets:
    s3.create_bucket(Bucket=bucket_name)
    print(f"Bucket {bucket_name} created")
else:
    print(f"Bucket {bucket_name} already exists")