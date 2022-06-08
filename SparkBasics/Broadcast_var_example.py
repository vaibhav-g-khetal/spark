import os
import sys
from pyspark.sql import *

os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

if __name__ == "__main__":
    spark = SparkSession.builder.appName("BroadCast_var example").getOrCreate()

    states = {"NY": "New York", "CA": "California", "FL": "Florida"}
    cols =["First_Name","Last_name", "Country", "State"]
    data = [("James", "Smith", "USA", "CA"),
            ("Michael", "Rose", "USA", "NY"),
            ("Robert", "Williams", "USA", "CA"),
            ("Maria", "Jones", "USA", "FL")
            ]
    broadcast = spark.sparkContext.broadcast(states)

    def state_convert(str):
        return broadcast.value[str]


    # Broadcast variable example in spark RDD
    rdd = spark.sparkContext.parallelize(data)
    result = rdd.map(lambda x: (x[0],x[1],x[2], state_convert(x[3]))).collect()
    print("Broadcast example in spark RDD")
    for i in result:
        print(i)

    # Broadcast variable example in spark DF
    df = spark.createDataFrame(data, cols)

    result = df.rdd.map(lambda x:(x[0],x[1],x[2], state_convert(x[3]))).toDF(cols)
    print("Broadcast variable example in Dataframe")
    result.show()




