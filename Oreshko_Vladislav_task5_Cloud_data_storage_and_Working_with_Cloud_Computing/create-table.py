from pyspark.sql.types import *
from pyspark.sql import SparkSession

# Создание Spark-сессии
spark = SparkSession.builder \
    .appName("create-table") \
    .getOrCreate()

# Создание схемы данных
schema = StructType([StructField('Name', StringType(), True),
StructField('Capital', StringType(), True),
StructField('Area', IntegerType(), True),
StructField('Population', IntegerType(), True)])

# Создание датафрейма
df = spark.createDataFrame([('Австралия', 'Канберра', 7686850, 19731984),
                             ('Австрия', 'Вена', 83855, 7700000)], schema)

df.show()

# Запись в бакет в формате Parquet
df.write.mode("overwrite").parquet("s3a://airflow-dataproc/countries/")

print("Job completed successfully!")
spark.stop()
