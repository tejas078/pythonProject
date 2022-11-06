from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("RDD Test").getOrCreate()
    input_rdd = spark.sparkContext.parallelize([1,2,3,5,6])
    print(input_rdd.collect())