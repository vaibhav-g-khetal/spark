import os
import sys
from pyspark.sql import *
from pyspark.sql.functions import expr
from pyspark.sql.functions import sum,avg,max,min,mean,count

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.appName("hello_spark").getOrCreate()

    file_path = "C://Users//Vaibhav_Khetal//Documents//Datasets//Oscars.txt"

    oscar_df = spark.read.format("csv") \
                         .option("inferSchema", "true")\
                         .option("header","true")\
                         .option("delimiter","\t")\
                         .load(file_path)
    award_df = oscar_df.select("birthplace","date_of_birth","race_ethnicity","year_of_award","award")

    # 1. Check DOB quality.Note that length varies based on month name
    quality_check = award_df.select(expr("length(date_of_birth)")).distinct()

    lengh_check = award_df.select(expr("length(date_of_birth)"))
    lengh_check.show()

