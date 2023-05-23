from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import boto3
from io import StringIO
import pandas
import logging

if __name__ == '__main__':
    spark = SparkSession.builder.master("local[*]").appName("test2").getOrCreate()
    logging.basicConfig(filename='C:/Users/Tejas/PycharmProjects/pythonProject/logs1.txt',
                        level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    driver = "oracle.jdbc.driver.OracleDriver"
    url = "jdbc:oracle:thin:@//192.168.0.84:1521/xe"
    user = "system"
    password = "system"
    spark.sparkContext.addFile(r"C:\Users\Tejas\Desktop\spark-3.3.0-bin-hadoop3\jars\ojdbc8.jar")


    try:
        df_73 = spark.read.format("jdbc").option("url", url) \
             .option("dbtable", "table_73") \
             .option("user", user) \
             .option("password", password) \
             .option("driver", driver).load()
    except Exception as e:
        logger.error(e)
    try:
        df_75 = spark.read.format("jdbc").option("url", url) \
             .option("dbtable", "table_75") \
             .option("user", user) \
             .option("password", password) \
             .option("driver", driver).load()
    except Exception as e:
        logger.error(e)

    df = df_73.unionAll(df_75)
    # print(df.count())

    # df.show()

    s3 = boto3.client("s3")
    csv_buffer = StringIO()
    panda_df = df.toPandas()
    panda_df.to_csv(csv_buffer, header=True, index=False, sep="|")
    bucket = 'dattasirtask2'
    try:
        s3.put_object(Bucket=bucket, Body=csv_buffer.getvalue(), Key='test.csv')
        logger.info(f"File uploaded to bucket {bucket} successfully")
    except Exception as e:
        logger.error(e)