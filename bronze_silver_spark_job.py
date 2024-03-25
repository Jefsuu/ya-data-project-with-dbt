import os
from kafka import KafkaConsumer, TopicPartition
import queue
import json
import mysql.connector
from kubernetes import client, config
import yaml
from datetime import datetime
import base64

config.load_incluster_config()
k8s_api = client.CoreV1Api()

# os.environ["BOOTSTRAP_SERVERS"] = 'localhost:9095'
# os.environ["KAFKA_TOPIC"] = 'minio_bronze_events'
# os.environ["KAFKA_GROUP_ID"] = 'spark_bronze_silver'
# os.environ["MYSQL_DATABASE_HOST"] = "localhost"
# os.environ["MYSQL_DATABASE"] = "topics"
# os.environ["MYSQL_USER"] = "root"
# os.environ["MYSQL_PASSWORD"] = "root1234"
# os.environ["MINIO_SECRET_NAME"] = "minio-access-secret"

process_queue = queue.Queue()
event_queue = queue.Queue()
job_queue = queue.Queue()

topic = os.environ.get("KAFKA_TOPIC")
# Criar o consumidor
consumer = KafkaConsumer(topic,
                         group_id=os.environ.get("KAFKA_GROUP_ID"),
                         bootstrap_servers=os.environ.get("BOOTSTRAP_SERVERS"),
                         auto_offset_reset='earliest',
                         enable_auto_commit=False,
                         max_poll_interval_ms=10000)

# Subscrever ao tópico
consumer.subscribe(topic)

last_offset = consumer.end_offsets([TopicPartition(topic, 0)])[TopicPartition(topic, 0)]
last_commited = consumer.committed(TopicPartition(topic, 0))
if last_offset == last_commited:
    print('last event')
    consumer.close()
else:
    # Ler as mensagens
    for message in consumer:
        print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                message.offset, message.key,
                                                message.value))
        # Comitar manualmente o offset após processar a mensagem
        event_queue.put(message.value.decode())
        consumer.commit()
        last_offset = consumer.end_offsets([TopicPartition(topic, 0)])[TopicPartition(topic, 0)]
        last_commited = consumer.committed(TopicPartition(topic, 0))
        if last_offset == last_commited:
            print('last event')
            consumer.close()
            break

topics_db = mysql.connector.connect(
    host=os.environ.get("MYSQL_DATABASE_HOST"),
    user=os.environ.get("MYSQL_USER"),
    password=os.environ.get("MYSQL_PASSWORD"),
    database=os.environ.get("MYSQL_DATABASE")
)
cursor = topics_db.cursor(buffered=True)

if not event_queue.empty():
    for _ in range(event_queue.qsize()):
        key_path = '/'.join(json.loads(event_queue.get())['Key'].split('/')[:-1])
        cursor.execute(
                f"""
                select 
                    bronze_path, 
                    silver_path, 
                    format, 
                    table_name,
                    read_options
                from table_config
                where bronze_path = '{key_path}'
                """
            )
        if cursor.rowcount > 0:
            result = cursor.fetchone()   
            job_parameters = {
                                "bronze_path":result[0],
                                "silver_path":result[1], 
                                "format":result[2], 
                                "table_name":result[3],
                                "read_options":result[4]
                            }
            job_queue.put(job_parameters)



def init_spark_job(parameters, namespace="dev", main_application_file = "s3a://script/bronze_silver_spark_job.py"):

    args = ["--bronze_path", parameters["bronze_path"],
            "--silver_path", parameters["silver_path"],
            "--format", parameters["format"],
            "--table_name", parameters["table_name"],
            "--read_options", parameters["read_options"]
            ]
    
    job_name = parameters["table_name"].replace(".", '-') + f"-{datetime.now().strftime('%Y%M%d%H%M%S')}"
    image = "apache/spark"

    spark_conf = {
        "spark.kubernetes.file.upload.path": "s3a://spark/jars/tmp",
        "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.EnvironmentVariableCredentialsProvider",
        "spark.hadoop.fs.s3a.fast.upload": "true",
        "spark.hadoop.fs.s3a.endpoint": "http://dev-minio:9000",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.ui.port": "4041",
        "spark.driver.extraJavaOptions": "-Divy.cache.dir=/tmp -Divy.home=/tmp",
        "hive.metastore.uris": "thrift://hive-metastore:9083"
    }
    
    minio_secret = k8s_api.read_namespaced_secret(os.getenv("MINIO_SECRET_NAME", "minio-access-secret"), os.getenv("KUBE_NAMESPACE", "dev"))


    minio_secret_env = [{"name": "AWS_ACCESS_KEY", "value": base64.b64decode(minio_secret.data['accessKey']).decode()},
                        {"name": "AWS_SECRET_KEY", "value": base64.b64decode(minio_secret.data['secretKey']).decode()}]
    
    # Define a especificação do job Spark Python
    job_spec = {
        "kind": "SparkApplication",
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "metadata": {
            "name": job_name,
            "namespace": namespace
        },
        "spec": {
            "type": "Python",
            "mode": "cluster",
            "image": image,
            "imagePullPolicy": "IfNotPresent",
            "mainApplicationFile": main_application_file,
            "sparkVersion": "3.3.0",
            "restartPolicy": {
                "type": "Never"
            },
            "driver": {
                "cores": 1,
                "coreLimit": "1200m",
                "memory": "512m",
                "labels": {
                    "version": "3.3.0"
                },
                "env": minio_secret_env,
                "serviceAccount": "dev-spark"
            },
            "executor": {
                "cores": 1,
                "instances": 1,
                "memory": "512m",
                "env": minio_secret_env
            },
            "sparkConf": spark_conf,
            "arguments": args,
            "deps": {"packages": ["org.apache.hadoop:hadoop-aws:3.2.2"]}
        }
    }

    # Criação do objeto do job Spark
    job_obj = client.CustomObjectsApi().create_namespaced_custom_object(
        group="sparkoperator.k8s.io",
        version="v1beta2",
        namespace=namespace,
        plural="sparkapplications",
        body=job_spec
    )

    print("Job Spark Python criado:", job_obj)

if not job_queue.empty():
    for _ in range(job_queue.qsize()):
        job_parameters = job_queue.get()

        init_spark_job(parameters=job_parameters)