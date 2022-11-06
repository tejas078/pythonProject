from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Empty Dataframe").getOrCreate()

    # empty rdd
    input_rdd = spark.sparkContext.emptyRDD()
    print(f"number of partitions: {input_rdd.getNumPartitions()}")
    header = ["name", "gender"]

    # custom schema
    schema_data = StructType([
        StructField("id", IntegerType(), False),
        StructField("name", StringType())
    ])

    # create empty dataframe from rdd
    df = spark.createDataFrame(input_rdd, schema_data)
    print(df.printSchema())

    df1 = input_rdd.toDF(schema_data)
    print(df1.printSchema())
