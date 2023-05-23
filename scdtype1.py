from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql import Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Aggregate Function").getOrCreate()

    schema_data = StructType([
        StructField("id", IntegerType(), False),  # nullable false
        StructField("name", StringType()),
        StructField("gender", StringType()),
        StructField("city", StringType()),
        StructField("salary", DoubleType())
    ])

    df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\input_data\employee.csv", schema=schema_data) \
        .rdd.zipWithIndex() \
        .filter(lambda x: x[1] > 3) \
        .map(lambda x: x[0]).toDF()

    df.show()