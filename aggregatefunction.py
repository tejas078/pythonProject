from pyspark.sql import SparkSession
from pyspark.sql.functions import *
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Salary").getOrCreate()

    data = [(1,"Ram",25,20000),(2,"Sham",29,30000),(3,"Kiran",31,10000),(4,"Mina",34,14000)]
    header = ["id","Name","Age","Salary"]

    input_rdd = spark.sparkContext.parallelize(data)
    input_df = input_rdd.toDF(header)
    # input_df.show()
    # input_df.printSchema()

    # get employee name who has max salary
    # input_df.select(max(input_df.Salary)).show()

    # get sum of salary from the file
    # input_df.select(sum(col("Salary"))).show()

    # get employee name, age who has age greater than 30
    # input_df.filter(input_df.Age>30).select("Name","Age").show()
    # input_df.select(aggregate(struct(input_df.Name,max(input_df.Salary)))).groupBy(input_df.Name).show()

    # input_df1 = input_df.select(max(input_df.Salary).alias("Salary"))
    # input_df.join(input_df1, "Salary").select("Name", "Salary").show()


    input_df.createOrReplaceTempView("table")
    spark.sql("select * from table").show()

    input_df.select("*").where(col("Age")==29).show()
    input_df.filter(col("Salary")>20000).show()
    input_df.withColumn("Minus_Salary",col("Salary")-10000).drop("Name").show()