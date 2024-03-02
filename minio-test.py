from minio import Minio
from minio.error import S3Error
client = Minio(
    "localhost:9000",
    access_key="Y8uG0MWV68ORrHZroeCy",
    secret_key="wYac4cJcnhR3AJfj47t7inKMeb9Ac33nerpwwMQ4",
    secure=False,
)

found = client.bucket_exists("bronze")
# if not found:
#     client.make_bucket("mybucket")
# else:
#     print("Bucket 'mybucket' already exists")

print(found)
# print(list(client.list_objects("mybucket")))