from minio import Minio
from minio.error import S3Error
from kubernetes import client, config, utils
import base64
import logging
import argparse
import glob
import os
import pathlib

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

parser = argparse.ArgumentParser()

parser.add_argument("-m", "--mode", required=True, choices=["A", "O"])
parser.add_argument("-sp", "--source_path", required=False)
parser.add_argument("-op", "--output_path", required=False)
parser.add_argument("-cb", "--create_bucket")
parser.add_argument("-bn", "--bucket_name")
parser.add_argument("-fp", "--folder_path")
parser.add_argument("--output_prefix")

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


def upload_file(bucket_name:str, file_name:str, file_path:str, prefix=None, minio_client=minio_client):
    
    if file_path == None:
        raise Exception("data cannot be None")
    if prefix == None:
        file_name = file_name
    else: 
        file_name = f"{prefix}/{file_name.split('.')[0]}/{file_name}"
    
    logger.info(f"uploading on bucket: {bucket_name}")
    minio_client.fput_object(bucket_name, object_name=file_name, file_path=file_path)



def list_files(folder:str) -> list:
    files = []
    for root_folder, sub_folders, file_names in os.walk(folder):
        for file_name in file_names:
            files.append(os.path.join(root_folder, file_name))
    logger.info(f"FILES: {files}")
    return files

if __name__ == "__main__":
    args = parser.parse_args()
    if args.mode == 'O':
        upload_file(bucket_name=args.bucket_name, file_name=args.output_path, file_path=args.source_path)

    elif args.mode == 'A':
        for file in list_files(args.folder_path):
            upload_file(bucket_name=args.bucket_name, file_name=file.split("/")[-1], file_path=file, prefix=args.output_prefix)


