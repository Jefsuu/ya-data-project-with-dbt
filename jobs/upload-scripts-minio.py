from minio import Minio
from minio.error import S3Error
from kubernetes import client, config, utils
import base64

k8s_client = client.ApiClient()
k8s_api = client.CoreV1Api()
config.load_kube_config()

minio_access_secret = k8s_api.read_namespaced_secret("minio-access-secret", "dev")
#minio_admin_secret = base64.b64decode(minio_admin_secret.data['root-password'])
#minio_admin_secret.decode()


client = Minio(
    "localhost:9000",
    access_key=base64.b64decode(minio_access_secret.data['accessKey']).decode(),
    secret_key=base64.b64decode(minio_access_secret.data['secretKey']).decode(),
    secure=False,
)

found = client.bucket_exists("bronze")
# if not found:
#     client.make_bucket("mybucket")
# else:
#     print("Bucket 'mybucket' already exists")

print(found)
# print(list(client.list_objects("mybucket")))