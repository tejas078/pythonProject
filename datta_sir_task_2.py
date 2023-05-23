from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import logging

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
    # logging.basicConfig(filename='C:/Users/Tejas/PycharmProjects/pythonProject/logs.txt',
    #                     level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    # logger = logging.getLogger(__name__)

    driver = "oracle.jdbc.driver.OracleDriver"
    url = "jdbc:oracle:thin:@//192.168.50.243:1521/xe"

    spark.sparkContext.addFile(r"C:\Users\Tejas\Desktop\spark-3.3.0-bin-hadoop3\jars\ojdbc8.jar")

    table_name = "SYSTEM.test_data_2"

    properties = {
        "driver": "oracle.jdbc.driver.OracleDriver",
        "user": "system",
        "password": "system",
    }
    schema = StructType([
        StructField("State_code", StringType(), True),
        StructField("Table_number", StringType(), True),
        StructField("Effective_date", StringType(), True),
        StructField("Exp_date", StringType(), True),
        StructField("Terr_code", StringType(), True),
        StructField("Amount", StringType(), True),
        StructField("deductible", StringType(), True),
        StructField("Symbol", StringType(), True),
        StructField("Factor", FloatType(), True)
    ])

    df = spark.read.csv(r"C:\Users\Tejas\Desktop\python\aws\datta_sir_task\Data_CSV.csv", header=True, schema=schema)

    df.write.jdbc(url=url, table=table_name, mode="overwrite", properties=properties)
