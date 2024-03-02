import logging
import os
from pyspark import SparkContext
from pyspark.sql import SparkSession

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("MinioSparkJob")
spark = SparkSession.builder.getOrCreate()
def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set("park.jars.packages", "org.apache.hadoop:hadoop-aws:3.2.2")
    spark_context._jsc.hadoopConfiguration().set("spark.hadoop.fs.s3a.aws.credentials.providery", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.access.key", "Y8uG0MWV68ORrHZroeCy")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.secret.key","wYac4cJcnhR3AJfj47t7inKMeb9Ac33nerpwwMQ4")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://10.244.0.16:9000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.attempts.maximum", "1")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.establish.timeout", "5000")
    spark_context._jsc.hadoopConfiguration().set("fs.s3a.connection.timeout", "10000")
load_config(spark.sparkContext)

# Read CSV file from MinIO
df = spark.read.option("header", "true").csv("s3a://stage/teste.csv")

total_rows_count = df.count()

df.write.format("csv").option("header", "true").save(os.getenv("OUTPUT_PATH", "s3a://bronze/teste.csv"))
logger.info(f"Total Rows for NYC Taxi Data: {total_rows_count}")
