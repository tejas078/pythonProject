from pyspark.sql import SparkSession
from pyspark.sql import Row

if __name__ == "__main__":
    spark = SparkSession.builder.master("local[*]").appName("Row class").getOrCreate()

    input_data = [Row(id=1,name="Ram",address=Row(city="Pune",state="MH")),
                  Row(id=2,name="Shyam",address=Row(city="Nagar",state="MH"))]

    df = spark.createDataFrame(input_data)
    df.show()
    df.printSchema()