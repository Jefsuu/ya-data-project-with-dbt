from minio import Minio
from minio.error import S3Error
from kubernetes import client, config, utils
import base64
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


config.load_kube_config()
k8s_client = client.ApiClient()
k8s_api = client.CoreV1Api()

minio_access_secret = k8s_api.read_namespaced_secret("minio-access-secret", "dev")

minio_client = Minio(
    "localhost:9000",
    access_key=base64.b64decode(minio_access_secret.data['accessKey']).decode(),
    secret_key=base64.b64decode(minio_access_secret.data['secretKey']).decode(),
    secure=False,
)


def create_bucket_if_not_exists(bucket_name:str, minio_client=minio_client) -> None:

    if not minio_client.bucket_exists(bucket_name):
        logger.info(f"creating bucket: {bucket_name}")
        minio_client.make_bucket(bucket_name)
    else:
        logger.info(f"bucket {bucket_name} already exists")


def upload_file(bucket_name:str, file_name:str, file_path:str, upload_path=None, minio_client=minio_client):
    
    if upload_path == None:
        file_name = file_name
    else:
        file_name = f"{upload_path}/{file_name}"
    
    if file_path == None:
        raise Exception("data cannot be None")
    
    logger.info(f"uploading on bucket: {bucket_name}")
    minio_client.fput_object(bucket_name, object_name=file_name, file_path=file_path)

create_bucket_if_not_exists("script")

upload_file(bucket_name="script", file_name="spark-minio.py", file_path="jobs/scripts/spark-minio.py")