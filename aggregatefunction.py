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

    df = spark.read.load(r"C:\Users\Tejas\PycharmProjects\pythonProject\input_data\employee.csv",
                         format="csv", schema=schema_data)
    # df.select(avg("salary")).show()
    # df.select(sum("salary")).show()
    # df.select(max("salary")).show()
    # df.select(min("salary")).show()
    # df.select(count("salary")).show()

    windowspec = Window.partitionBy("gender").orderBy("salary")

    # df.withColumn("row_number", row_number().over(windowspec)).show()
    # df.withColumn("rank", rank().over(windowspec)).show()
    # df.withColumn("dense_rank", dense_rank().over(windowspec)).show()

    df.withColumn("count", count(col("salary")).over(windowspec)).show()
    # df.withColumn("lag", lag("salary", 2).over(windowspec)).show()