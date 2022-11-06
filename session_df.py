from pyspark.sql import SparkSession
if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("Dataframe session").getOrCreate()

    data = [("Ram","Male"),("Shyam","Male"),("Kiran","Female")]

    data1 = [("Ram", "Male"), ("Shyam", "Male"), ("Kiran", "male")]
    header = ["name","gender"]
    input_rdd = spark.sparkContext.parallelize(data)
    input_rdd1 = spark.sparkContext.parallelize(data1)
    input_df = input_rdd.toDF()
    #input_df.show() --without column names

    input_df1 = input_rdd.toDF(header)
    #input_df1.show()

#    input_df1.printSchema()

    input_df2 = spark.createDataFrame(data1,schema=header)

    #input_df2.show()

    csv_df = spark.read.csv(r"C:\Users\Tejas\PycharmProjects\pythonProject\input_data\employee.csv")
    csv_df.show()