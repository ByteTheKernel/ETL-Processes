"""
process_kafka_messages.py — читает Kafka или JSONL из S3, flatten JSON, пишет Parquet.
Для внутренних VPC соединений использует порт 9092 (SASL_PLAINTEXT, без SSL).
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType, ArrayType
)

DOCUMENT_SCHEMA = StructType([
    StructField("type",   StringType(), True),
    StructField("status", StringType(), True),
])

MSG_SCHEMA = StructType([
    StructField("application_id", StringType(), True),
    StructField("customer", StructType([
        StructField("customer_id", StringType(), True),
        StructField("region",      StringType(), True),
    ]), True),
    StructField("loan", StructType([
        StructField("amount",      LongType(),    True),
        StructField("term_months", IntegerType(), True),
    ]), True),
    StructField("scoring", StructType([
        StructField("score",      IntegerType(), True),
        StructField("risk_level", StringType(),  True),
    ]), True),
    StructField("documents",       ArrayType(DOCUMENT_SCHEMA), True),
    StructField("decision_status", StringType(), True),
    StructField("submitted_at",    StringType(), True),
])


def flatten_df(df):
    return (
        df
        .select(
            "application_id",
            F.col("customer.customer_id").alias("customer_id"),
            F.col("customer.region").alias("region"),
            F.col("loan.amount").alias("loan_amount"),
            F.col("loan.term_months").alias("term_months"),
            F.col("scoring.score").alias("credit_score"),
            F.col("scoring.risk_level").alias("risk_level"),
            "decision_status",
            F.to_timestamp("submitted_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("submitted_at"),
            "documents",
        )
        .withColumn("document",   F.explode_outer("documents"))
        .withColumn("doc_type",   F.col("document.type"))
        .withColumn("doc_status", F.col("document.status"))
        .drop("documents", "document")
        .withColumn("is_approved",
                    F.when(F.col("decision_status") == "approved", 1).otherwise(0))
        .withColumn("event_date",  F.to_date("submitted_at"))
        .withColumn("event_month", F.date_format("submitted_at", "yyyy-MM"))
        .withColumn("event_year",  F.year("submitted_at"))
    )


def read_from_kafka(spark, brokers, topic, username, password):
    """
    Читает Kafka через SASL_PLAINTEXT (порт 9092) — внутреннее VPC соединение.
    Не требует SSL-сертификата — DataProc и Kafka в одной VPC сети.
    """
    jaas = (
        f'org.apache.kafka.common.security.scram.ScramLoginModule required '
        f'username="{username}" password="{password}";'
    )
    print(f"Connecting to Kafka (SASL_PLAINTEXT): {brokers} topic={topic}")
    return (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers",   brokers)
        .option("subscribe",                 topic)
        .option("startingOffsets",           "earliest")
        # SASL_PLAINTEXT — без SSL, только для внутренних VPC соединений
        .option("kafka.security.protocol",   "SASL_PLAINTEXT")
        .option("kafka.sasl.mechanism",      "SCRAM-SHA-512")
        .option("kafka.sasl.jaas.config",    jaas)
        .load()
        .select(
            F.from_json(F.col("value").cast("string"), MSG_SCHEMA).alias("data")
        )
        .select("data.*")
    )


def read_from_s3(spark, input_path):
    return spark.read.json(input_path, schema=MSG_SCHEMA, multiLine=False)


def write_results(flat_df, output_path):
    flat_out = output_path.rstrip("/") + "/flat/"
    agg_out  = output_path.rstrip("/") + "/aggregated/"

    flat_df.coalesce(4).write.mode("overwrite").parquet(flat_out)
    print(f"Flat table → {flat_out}")

    agg = (
        flat_df
        .groupBy("region", "risk_level", "event_month")
        .agg(
            F.count("application_id").alias("total_messages"),
            F.countDistinct("application_id").alias("unique_applications"),
            F.sum("is_approved").alias("approved_count"),
            F.round(F.avg("credit_score"), 1).alias("avg_credit_score"),
            F.round(F.avg("loan_amount"),  0).alias("avg_loan_amount"),
            F.sum("loan_amount").alias("total_loan_volume"),
        )
        .withColumn("approval_rate",
                    F.round(F.col("approved_count") / F.col("total_messages") * 100, 2))
        .orderBy("event_month", "region", "risk_level")
    )
    agg.coalesce(2).write.mode("overwrite").parquet(agg_out)
    print(f"Aggregated → {agg_out}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode",   choices=["s3", "kafka"], default="s3")
    parser.add_argument("--input",  default=None)
    parser.add_argument("--output", required=True)
    parser.add_argument("--kafka-brokers",  default=None)
    parser.add_argument("--kafka-topic",    default="loan-applications")
    parser.add_argument("--kafka-username", default="kafka-user")
    parser.add_argument("--kafka-password", default="Kafka!2026")
    args = parser.parse_args()

    spark = (
        SparkSession.builder
        .appName("KafkaMessagesETL")
        .config("spark.sql.shuffle.partitions", "20")
        .config("spark.hadoop.fs.s3a.endpoint", "https://storage.yandexcloud.net")
        .config("spark.hadoop.fs.s3a.impl",     "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    if args.mode == "kafka":
        assert args.kafka_brokers, "--kafka-brokers required"
        df = read_from_kafka(spark, args.kafka_brokers, args.kafka_topic,
                             args.kafka_username, args.kafka_password)
    else:
        assert args.input, "--input required in s3 mode"
        print(f"Reading from S3: {args.input}")
        df = read_from_s3(spark, args.input)

    count = df.count()
    print(f"Messages read: {count:,}")

    flat = flatten_df(df)
    print(f"Rows after flatten: {flat.count():,}")

    write_results(flat, args.output)
    print("Done!")
    spark.stop()


if __name__ == "__main__":
    main()
