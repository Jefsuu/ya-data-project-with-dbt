import logging
import os
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
import argparse



logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger("SparkJob")

parser =  argparse.ArgumentParser()

parser.add_argument('--bronze_path')
parser.add_argument('--silver_path')
parser.add_argument('--table_name')
parser.add_argument('--format')

args = parser.parse_args()


conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.2')
conf.set("hive.metastore.uris", "thrift://hive-metastore:9083")
spark = (
        SparkSession
        .builder
        .enableHiveSupport()
        .config(conf=conf)
        .getOrCreate()
    )

logger.info(args)

df = spark.read.format(args.format).load(f"s3a://{args.bronze_path}")

df.write.mode('overwrite').format('parquet').option("path", f"s3a://{args.silver_path}").saveAsTable(args.table_name)