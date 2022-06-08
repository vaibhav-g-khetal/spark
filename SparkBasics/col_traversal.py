import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import sum,avg,max,min,mean,count

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.appName("hello_spark").getOrCreate()

    data_list = [("Ravi", 28),
                 ("David", 45),
                 ("abdul", 37)
                 ]

    df = spark.createDataFrame(data_list).toDF("Emp Name", "Emp Age")

    for cols in df.columns:
        cols.join("_")

    df.show()