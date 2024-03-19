mc admin config set local/ notify_kafka:minio_bronze_events \
    brokers="dev-kafka:9092" \
    topic="minio_bronze_events" 

mc admin service restart local

mc admin info --json local | jq  .info.sqsARN

mc event add local/bronze arn:minio:sqs::minio_bronze_events:kafka \
  --event put