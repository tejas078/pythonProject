from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from pyspark.sql.functions import *
from pyspark.sql import Window

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Aggregate Function").getOrCreate()

    schema_data = StructType([
        StructField("firstname", StringType()),
        StructField("lastname", StringType()),
        StructField("country", StringType()),
        StructField("state", StringType())
    ])

    test_rdd = spark.sparkContext.textFile("s3a://test-bucket-031/statesData.csv")

    states = {"CA": "California", "NY": "New York", "FL": "Florida"}

    broadcast_var = spark.sparkContext.broadcast(states)

    def state_change(state_code):
        return broadcast_var.value[state_code]

    rdd = test_rdd.map(lambda x:x.split(","))

    final = rdd.map(lambda x:(x[0],x[1],x[2], state_change(x[3])))

    df = final.toDF(schema_data)
    df.write.csv("s3a://test-bucket-031/statesData1.csv")