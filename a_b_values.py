from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql import Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Aggregate Function").getOrCreate()

    schema_data = StructType([
        StructField("col1", StringType()),
        StructField("colA", StringType()),
        StructField("colB", StringType()),
    ])

    schema_data1 = StructType([
        StructField("colA_a", StringType()),
        StructField("colB_b", StringType()),
    ])

    df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\a_bData.csv", schema=schema_data)

    df1 = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\a_b1Data.csv", schema=schema_data1)

    for column in [column for column in df1.columns]:

        df = df.withColumn(column, lit(None))

    df.show()