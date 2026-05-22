from pyspark.sql import SparkSession

KAFKA_BOOTSTRAP = "rc1b-0m5o10ck2c71qma0.mdb.yandexcloud.net:9091"
KAFKA_TOPIC = "hw15-topic"
KAFKA_USER = "kafka-user"
KAFKA_PASSWORD = "secret"

spark = SparkSession.builder \
    .appName("KafkaHW15") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df = spark.read \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP) \
    .option("kafka.security.protocol", "SASL_SSL") \
    .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
    .option("kafka.sasl.jaas.config",
            f'org.apache.kafka.common.security.scram.ScramLoginModule required '
            f'username="{KAFKA_USER}" password="{KAFKA_PASSWORD}";') \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

messages = df.selectExpr(
    "CAST(key AS STRING) as key",
    "CAST(value AS STRING) as value",
    "topic",
    "partition",
    "offset",
    "timestamp"
)

messages.show(truncate=False)
print(f"Всего сообщений прочитано: {messages.count()}")

spark.stop()