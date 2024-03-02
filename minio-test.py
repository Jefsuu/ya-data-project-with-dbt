from minio import Minio
from minio.error import S3Error
client = Minio(
    "localhost:9000",
    access_key="eWPnZFJ0aK0WHBEn4DQZ",
    secret_key="XyzlHA7iL5JihAmzkoar0GP1gwnTemEj9ojMolUK",
    secure=False,
)

found = client.bucket_exists("bronze")
# if not found:
#     client.make_bucket("mybucket")
# else:
#     print("Bucket 'mybucket' already exists")

print(found)
# print(list(client.list_objects("mybucket")))