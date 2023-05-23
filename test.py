from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Row
from pyspark.sql.window import Window
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()

    # schema_data = StructType([
    #     StructField("emp_id", IntegerType()),
    #     StructField("emp_name", StringType()),
    #     StructField("location_1", StringType()),
    #     StructField("location_2", StringType()),
    #     StructField("location_3", StringType())
    # ])

    #df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test1.csv", schema=schema_data)
    # df.show()
    # unPivotDF = df.select("emp_id","emp_name",expr("stack(3, location_1, location_2, location_3) as (Location)"))\
    #     .where("Location is not null")
    # unPivotDF.show()
    # def capStr(name):
    #     return name.title()
    # spark.udf.register(name="capStr", f=capStr, returnType=StringType())
    # df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test1.csv")
    # df1 = df.rdd.map(lambda x: (x[0],capStr(x[1]))).toDF()
    # df1.show()
    # #or
    # # df.createTempView("df_tbl")
    # # df1 = spark.sql("select _c0,capStr(_c1) name from df_tbl")
    #
    # df1 = df1.withColumn("newCol",lit("dummy")).where("name = 'Yuvraj Singh'")
    # df1.show()

    # schema_data = StructType([
    #     StructField('emp_id', IntegerType()),
    #     StructField('emp_address', StringType()),
    #     StructField('state', StringType())
    # ])
    #
    # data = {"Rd": "Road", "St": "Street", "Ave": "Avenue"}
    #
    # broadcast_var = spark.sparkContext.broadcast(data)
    # def func(addr):
    #     lst = addr.split()
    #     for i in range(len(lst)):
    #         if lst[i] in broadcast_var.value:
    #             lst[i] = broadcast_var.value[lst[i]]
    #     str1 = " ".join(lst)
    #     return str1
    # df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test2.csv", schema=schema_data)
    # df1 = df.rdd.map(lambda x:(x[0],func(x[1]),x[2])).toDF()
    # df1.show()

    # rdd = spark.sparkContext.textFile(r"C:\Users\Tejas\PycharmProjects\pythonProject\test3.txt")


    #df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test2.csv")
    # df.dropDuplicates().show() # or df.distinct().show()
    # rdd = spark.sparkContext.parallelize([1,2,3,4,5])
    # print(rdd.reduce(lambda x,y:x+y))

    #data = ["Project Gutenberg's","Alice's adventure in wonderland","Project Gutenberg's","adventures in wonderland","Project Gutenberg's"]
    # rdd = spark.sparkContext.parallelize(data)
    # rdd1 = rdd.reduce(lambda x,y:x.replace(" ", "/n")+y.replace(" ", "/n"))
    # rdd1.collect()
    # str1 = ""
    # for i in range(len(data)):
    #     if i < len(data) -1:
    #         str2 = data[i].replace(" ", "/n")
    #         str1 = str1 + str2
    #     else:
    #         str1 = str1 + "\n" + data[i]

    #str1 = ",".join(data).replace(",","/n").replace(" ","/n")
    # print(str1)
    #rdd = spark.sparkContext.parallelize(["Project Gutenberg's","Alice's adventure in wonderland","Project Gutenberg's","adventures in wonderland","Project Gutenberg's"])

    # data = [1,2,3,4,5]
    # rdd = spark.sparkContext.parallelize(data)
    # total = rdd.reduce(lambda x,y:x+y)
    # df = rdd.map(Row("nums")).toDF()
    # df.withColumn("Total",lit(total)).show()

    # data = ["Pyspark", "Interview", "Questions", "at", "Brainworks"]
    # rdd = spark.sparkContext.parallelize(data)
    # print(rdd.count())

    # df = spark.read.option("header","true").option("delimiter","~|").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test4.csv")
    # df.show()

    # rdd = spark.sparkContext.textFile(r"C:\Users\Tejas\PycharmProjects\pythonProject\test5.text")
    #
    # def split(text):
    #     values = text.split('|')
    #     for i in range(0, len(values), 5):
    #         if values[i] != "":
    #             yield values[i:i + 5]
    #
    # header = ["Name","Edu","YearsOfExp","Tech","Mob_num"]
    #
    # df = rdd.flatMap(split).toDF(header)
    # df.show()

    # def mask_num(num):
    #     n = len(num)
    #     lst = list(num)
    #     print(lst)
    #     lst[2:int(n)-2] = 'x' * int(n-4)
    #     return "".join(lst)
    #
    # mask_udf = udf(mask_num,StringType())
    #
    # df = spark.read.option("header","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test6.csv")
    # df = df.withColumn('email_masked', regexp_replace('email', '(?<!^).(?=.+@)', '*'))
    # # df = df.withColumn('mobile_masked', F.regexp_replace('mobile', '(?<!^).(?!$)', '*')) #or
    # df = df.withColumn('mobile_masked', mask_udf(df["mobile"]))
    # df = df.drop("mobile").drop("email")
    # df.show()

    # df = spark.read.option("header","true").option("mode","DROPMALFORMED").option("delimiter",".").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test7.csv")
    # df.show()

    # df = spark.read.option("header","true").option("inferschema","true").option("delimiter",",").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test8.csv")
    # df.show()
    # df1 = df.groupBy("Customer No").pivot("Transaction Type").sum("Amount")
    # df1.select(col("Customer No"), (col("credit") - col("debit")).alias("balance")).show()

    # df = spark.read.option("delimiter","|").option("header","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test9.csv")
    # df.show()
    # df.withColumn("Final_val", col("Size")*col("Price_SQ_FT")).show()
    # print(df.rdd.getNumPartitions())

    # rdd = spark.sparkContext.textFile(r"C:\Users\Tejas\PycharmProjects\pythonProject\test9.txt")
    # print(rdd.zipWithIndex().collect())
    # rdd1 = rdd.zipWithIndex().filter(lambda x:x[1] > 0).map(lambda x:x[0].split("|")).map(lambda x:(int(x[5])*float(x[6])))
    # print(rdd1.collect())
    # print(rdd.is_cached)

    # data = spark.sparkContext.textFile(r"C:\Users\Tejas\PycharmProjects\pythonProject\test9.csv").map(lambda x:x.split("|"))
    # header = data.first()
    # data = data.filter(lambda x: x != header)
    # print(data.collect())
    # data.coalesce(1).saveAsTextFile(r"C:\Users\Tejas\PycharmProjects\pythonProject")


#delete duplicates from df
    # df = spark.read.option("header","true").option("delimiter","~|").option("inferSchema","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test20.csv")
    # df1 = df.groupBy("Name","Age").count().where("count == 1").drop("count")
    # df1.show()
    #OR

    # window_spec = Window.partitionBy("Name").orderBy("Age")
    # df1 = df.withColumn("rn",row_number().over(window_spec)).filter("rn == 1").drop("rn")
    # df1.show()

    # rdd = spark.sparkContext.textFile(r"C:\Users\Tejas\PycharmProjects\pythonProject\test21.txt")
    # rdd.zipWithIndex().filter(lambda x:x[1] > 6).toDF(['value']).show()

    # df = spark.read.csv([r"C:\Users\Tejas\PycharmProjects\pythonProject\test1.csv",r"C:\Users\Tejas\PycharmProjects\pythonProject\test12.csv"])
    # df.show()
    # print(df.rdd.isEmpty())


    df = spark.read.option("header","true").option("inferschema","true").option("delimiter",",").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test8.csv")
    df.agg(max(col("Amount"))).show()
    df.select(max("Amount")).show()
    # df.cache()
    df.unpersist()
    # print(df.storageLevel.useMemory)
    # or
    # print(df.is_cached)
