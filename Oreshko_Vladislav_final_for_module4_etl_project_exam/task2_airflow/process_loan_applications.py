"""
process_loan_applications.py
PySpark ETL-задание для обработки кредитных заявок.

Читает loan_applications.csv из S3, выполняет трансформации
и записывает результат в S3 в формате Parquet.

Запуск вручную (для теста):
  spark-submit process_loan_applications.py \
    --input  s3a://etl-exam/raw/loan_applications.csv \
    --output s3a://etl-exam/processed/loan_applications/
"""

import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType, BooleanType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_schema():
    """Схема CSV-файла loan_applications."""
    return StructType([
        StructField("application_id",       StringType(),    True),
        StructField("event_time",           StringType(),    True),  # читаем как строку, потом кастуем
        StructField("customer_id",          StringType(),    True),
        StructField("region_code",          StringType(),    True),
        StructField("product_type",         StringType(),    True),
        StructField("requested_amount",     DoubleType(),    True),
        StructField("term_months",          IntegerType(),   True),
        StructField("credit_score",         IntegerType(),   True),
        StructField("risk_level",           StringType(),    True),
        StructField("decision_status",      StringType(),    True),
        StructField("approved_amount",      DoubleType(),    True),
        StructField("channel",              StringType(),    True),
        StructField("employee_review_flag", StringType(),    True),  # "true"/"false" → boolean
        StructField("processing_time_sec",  IntegerType(),   True),
    ])


def transform(df):
    """ETL-трансформации над датафреймом."""

    # 1. Привести event_time к TimestampType
    df = df.withColumn(
        "event_time",
        F.to_timestamp(F.col("event_time"), "yyyy-MM-dd HH:mm:ss")
    )

    # 2. Boolean из строки
    df = df.withColumn(
        "employee_review_flag",
        F.when(F.lower(F.col("employee_review_flag")) == "true", True).otherwise(False)
    )

    # 3. Производные поля
    df = df.withColumn("event_date", F.to_date(F.col("event_time")))
    df = df.withColumn("event_month", F.date_format(F.col("event_time"), "yyyy-MM"))
    df = df.withColumn("event_year",  F.year(F.col("event_time")))

    # 4. Процент одобрения от запрошенного
    df = df.withColumn(
        "approval_rate",
        F.when(
            (F.col("requested_amount").isNotNull()) & (F.col("requested_amount") > 0),
            F.round(F.col("approved_amount") / F.col("requested_amount"), 4)
        ).otherwise(F.lit(None).cast(DoubleType()))
    )

    # 5. Категория кредитного скора (FICO-подобная)
    df = df.withColumn(
        "credit_score_band",
        F.when(F.col("credit_score") >= 750, "excellent")
         .when(F.col("credit_score") >= 700, "good")
         .when(F.col("credit_score") >= 650, "fair")
         .when(F.col("credit_score") >= 600, "poor")
         .otherwise("very_poor")
    )

    # 6. Флаг: заявка одобрена
    df = df.withColumn(
        "is_approved",
        F.col("decision_status") == "approved"
    )

    # 7. Убрать строки без application_id (защита от мусора)
    df = df.filter(F.col("application_id").isNotNull())

    return df


def main():
    parser = argparse.ArgumentParser(description="Loan Applications ETL")
    parser.add_argument("--input",  required=True,  help="Входной путь S3 (CSV)")
    parser.add_argument("--output", required=True,  help="Выходной путь S3 (Parquet)")
    parser.add_argument("--partitions", type=int, default=4, help="Число партиций записи")
    args = parser.parse_args()

    logger.info(f"Input:  {args.input}")
    logger.info(f"Output: {args.output}")

    spark = (
        SparkSession.builder
        .appName("LoanApplicationsETL")
        # Настройки S3 — на кластере DataProc они уже прописаны,
        # но оставляем явно для наглядности
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.endpoint",
                "https://storage.yandexcloud.net")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logger.info("Reading CSV...")
    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .option("nullValue", "")
        .schema(get_schema())
        .csv(args.input)
    )

    logger.info(f"Rows read: {df.count()}")

    logger.info("Transforming...")
    df_out = transform(df)

    logger.info(f"Rows after transform: {df_out.count()}")

    # Партиционируем по месяцу для удобной работы в DataLens
    logger.info(f"Writing Parquet to {args.output} ...")
    (
        df_out
        .repartition(args.partitions, F.col("event_month"))
        .write
        .mode("overwrite")
        .partitionBy("event_year", "event_month")
        .parquet(args.output)
    )

    logger.info("Done!")

    # Краткая статистика в лог Spark
    df_out.groupBy("decision_status").count().show()
    df_out.groupBy("risk_level").count().show()

    spark.stop()


if __name__ == "__main__":
    main()
