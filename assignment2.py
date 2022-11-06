from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Test1").getOrCreate()

    df = spark.read.text(r"C:\Users\Tejas\Desktop\python\spark\testData.txt")
    df1 = df.withColumn('value', explode(split(col('value'), ' '))).groupBy('value')
    df2 = df1.count()
    df2.show()
