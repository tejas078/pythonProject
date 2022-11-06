from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType, DoubleType
from pyspark.sql.functions import col
from pyspark.sql.functions import lit

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("With Column").getOrCreate()

    # custom schema
    schema_data = StructType([
        StructField("id", IntegerType(), False),  # nullable false
        StructField("name", StringType()),
        StructField("gender", StringType()),
        StructField("city", StringType()),
        StructField("salary", DoubleType())
    ])

    df = spark.read.load(r"C:\Users\Tejas\PycharmProjects\pythonProject\input_data\employee.csv",
                         format="csv", schema=schema_data)

    #df.printSchema()
    #df.show()

    df1 = df.withColumn("salary", col("salary").cast("Integer"))  #changed datatype of column
    #df1.show()
    #df1.printSchema()
    #df.withColumn("salary", df.salary * 100).show()
    #or
    #df.withColumn("salary", col("salary") * 100).show()

    #assignment
    #add new column state with value MH

    df.withColumn("state",  lit("MH")).show()

    #rename column
    #df.withColumnRenamed("name", "first_name").printSchema()

    #drop column
    #df.drop("city").printSchema()