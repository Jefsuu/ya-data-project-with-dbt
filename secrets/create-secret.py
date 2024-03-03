import json
import base64
from kubernetes import client, config, utils


config.load_kube_config()

api_instance = client.CoreV1Api()
sec  = client.V1Secret()

file = open("secrets/credentials.json")
secret = json.load(file)

def create_secret(name:str, data:dict, keys:list[str], namespace:str, type='Opaque') -> None:

    secret_value = {}
    for k in keys:
        secret_value[k] = base64.b64encode(data[k].encode('utf-8')).decode('utf-8')

    sec.metadata = client.V1ObjectMeta(name=name)
    sec.type = type
    sec.data = secret_value

    api_instance.create_namespaced_secret(namespace=namespace, body=sec)

create_secret(name="minio-access-secret", keys=['accessKey', 'secretKey'], data=secret, namespace="dev")


