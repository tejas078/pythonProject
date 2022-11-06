from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Filter data").getOrCreate()

    schema_data = StructType([
        StructField("id", IntegerType(), False),  # nullable false
        StructField("name", StringType()),
        StructField("gender", StringType()),
        StructField("city", StringType()),
        StructField("salary", DoubleType())
    ])

    df = spark.read.load(r"C:\Users\Tejas\PycharmProjects\pythonProject\input_data\employee.csv",
                         format="csv", schema=schema_data)

    #df.filter(df.gender == "male").select("id", "name", "gender").show()

    # df.filter(df.gender.startswith("m")).show()
    df1 = df.withColumn("salary", col("salary").cast("Integer"))
    df1.printSchema()
    df1.filter(max(df1.salary)).show()