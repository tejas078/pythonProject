from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("mock test").getOrCreate()

    schema_data = StructType([
        StructField("c1", StringType()),
        StructField("c2", StringType())
    ])

    schema_data1 = StructType([
        StructField("dept_no", IntegerType()),
        StructField("emp_no", StringType())
    ])

    schema_data2 = StructType([
        StructField("col1", StringType()),
        StructField("col2", StringType()),
        StructField("col3", StringType())
    ])

    df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\input_data\testData.csv", schema=schema_data)

    #df3 = df.select("c1","c2").withColumn("c3", expr("case when c1 = '123' then c2 " "when c2 = '123' then c1 " "end"))

    df.show()

    #df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\input_data\listaggData.csv", schema=schema_data1)
    #df.orderBy("dept_no").groupby("dept_no").agg(array_join(collect_list('emp_no'), delimiter=",").alias("emp_nos")).show()

    # df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\input_data\nullvalues.csv", schema=schema_data2)
    #
    # df.withColumn("output", expr("case when col1 != 'null' then col1 " "when col2 != 'null' then col2 " "when col3 != 'null' then col3 " "end")).select("output").show()