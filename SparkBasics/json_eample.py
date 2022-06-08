import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

path = "C://Users//Vaibhav_Khetal//Desktop//json_sample.json"
spark = SparkSession.builder.appName("json_example").getOrCreate()
json_df = spark.read.json(path)

ageInfo_df = json_df.select("npi_number", "people_id", explode("age_info").alias("age_info"))
ageInfo_df = ageInfo_df.withColumn("age_per", ageInfo_df["age_info.percentage"])\
    .withColumn("age_range", ageInfo_df["age_info.range"])

raceInfo_df = json_df.select("npi_number", "people_id", explode("race_info").alias("race_info"))
raceInfo_df = raceInfo_df.withColumn("race_per", raceInfo_df["race_info.percentage"])\
    .withColumn("race", raceInfo_df["race_info.race"])

sexInfo_df = json_df.select("npi_number", "people_id", explode("sex_info").alias("sex_info"))
sexInfo_df = sexInfo_df.withColumn("sex_per", sexInfo_df["sex_info.percentage"])\
    .withColumn("value", sexInfo_df["sex_info.value"])
sexInfo_df.show()
