from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    # data = "test hai bhai test"
    # rdd = spark.sparkContext.parallelize([data])
    # rdd1 = rdd.flatMap(lambda x:x.split(" "))
    # rdd2 = rdd1.map(lambda x:(x,1))
    # rdd3 = rdd2.reduceByKey(lambda x,y:x+y)
    # print(rdd3.collect())

    # df = spark.read.option("header","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test11.csv")
    # df1 = df.distinct()
    # df1.show()

    # df = spark.read.option("header","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test12.csv")
    # df.groupBy("id").count().where("count > 1").show()

    # df = spark.read.option("header","true").option("inferschema","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test13.csv")
    # df.groupBy("cmpny").pivot("qtr").sum("sales").show()

    # df = spark.read.option("header","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test14.csv")
    # df.select(split(col("col1"),"[|]").getItem(0).alias("col1"),split(col("col1"),"[|]").getItem(1).alias("col2"),split(col("col1"),"[|]").getItem(2).alias("col3")).show()

    # df = spark.read.option("header","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test15.csv")
    # df.withColumn("full_name",concat(col("first_name"),lit(" "),col("last_name"))).withColumn("age_b",when(col("age") > 50, "Y").otherwise("N")).show()

    # schema_data = StructType([
    #     StructField("ID", IntegerType()),
    #     StructField("Date", StringType()),
    #     StructField("Amount", IntegerType())
    # ])
    #
    # Data = [Row(ID=1, Date='1992-1-27', Amount=50000),
    #         Row(ID=1, Date='1992-1-3', Amount=20000),
    #         Row(ID=2, Date='1992-2-3', Amount=25000),
    #         Row(ID=2, Date='1992-3-2', Amount=2000)
    #     ]
    #
    # df = spark.createDataFrame(Data,schema_data)
    # df1 = df.groupBy("ID").sum("Amount").alias("TotalAmount")
    # df1.show()
    # df1.write.option("header","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test16")

    # df = spark.read.option("header","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test16.csv")
    # df.select(mean("salary")).show()  #df.select(avg("salary")).show()

    # df = spark.read.option("header", "true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test16.csv")
    # df.na.fill("1000",["salary"]).show()

    # df = spark.read.option("header", "true").option("inferSchema", "true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test")  # takes all csv files from test folder
    # df = spark.read.option("header", "true").option("inferSchema", "true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test16.csv")
    # df1 = spark.read.option("header", "true").option("inferSchema", "true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test17.csv")
    # df2 = df1.unionByName(df)
    # df2.show()

    # df = spark.read.option("header","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test18.csv")
    # df.select("emp_id","emp_name",expr("stack(3,location_1,location_2,location_3) as (Location)")).where("Location is not null").show()

    # df = spark.read.option("header", "true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test15.csv")
    # df.withColumn("age",col("age").cast(IntegerType())).printSchema()

    # df = spark.read.option("header","true").option("inferSchema","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test19.csv")
    # df.groupBy("id").pivot("tag").sum("value").show()

    # df = spark.read.option("header", "true").option("inferSchema", "true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test17.csv")
    # window_spec = Window.partitionBy("dept").orderBy("salary")
    # df.withColumn("rank",row_number().over(window_spec)).where("rank == 2").show()