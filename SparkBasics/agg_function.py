import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import sum,avg,max,min,mean,count

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.appName("hello_spark").getOrCreate()

    data_list = [("Ravi", 23, "London"),
                 ("David", 43, "Madrid"),
                 ("John", 32, "London"),
                 ("Julie", 29, "Madrid"), ("Maria", 33, "Berlin")]
    columns = ["Name", "Age", "City"]
    df = spark.createDataFrame(data_list, columns)
    df = df.groupby("City").agg(sum("Age").alias("sum_age"),
                                avg("Age").alias("avg_age"),
                                min("Age").alias("min_age"),
                                max("Age").alias("max_age"))
    df.show()

    spark.stop()