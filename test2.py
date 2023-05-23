from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("test2").getOrCreate()

    # df = spark.read.option("header","true").option("delimiter","~|").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test20.csv")
    # df.show()

    # df = spark.read.option("header","true").option("delimiter"," ").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test21.csv")
    # df.select("id",split(col("fnamelname"),"[-,/,_]").getItem(0).alias("fnamelname"),split(col("fnamelname"),"[-,/,_]").getItem(1).alias("amount")).show()

    # df = spark.read.option("header","true").option("delimiter","~|").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test20.csv")
    # windowspec = Window.partitionBy("Name").orderBy("Age")
    # df1 = df.withColumn("rank",row_number().over(windowspec)).where("rank = 1")
    # df1.select("Name","Age").show()

    # df = spark.read.option("header","true").option("inferSchema","true").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test22.csv")
    # df.show()

    # wordsDF = spark.createDataFrame([('look',), ('spark',), ('tutorial',), ('spark',), ('look',), ('python',)],['word'])
    # wordsDF.groupBy("word").count().show()

    # df = spark.read.text(r"C:\Users\Tejas\PycharmProjects\pythonProject\test3.txt")
    # df.show()
    # df.withColumn("value",explode(split(col("value")," "))).groupBy("value").count().show()


    # df = spark.read.option("header","true").option("delimiter","|").csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\test23.csv")
    # df.select("id",expr("stack(3,'Name',Name,'Age',Age,'Salary',Salary) as (Col0,Col1)"),"insertedBy").show()

    # df = spark.read.text(r"C:\Users\Tejas\PycharmProjects\pythonProject\test3.txt")
    # df1 = df.filter(col("value").contains("PySpark"))
    # if df1.count() > 0:
    #     print("Found")
    # else:
    #     print("Not Found")