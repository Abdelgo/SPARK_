import findspark
findspark.init()

import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession

# May take awhile locally

if __name__ == "__main__":
    
    spark = SparkSession.builder.master("local[*]").appName("thesparkapp").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
    print(spark.sparkContext.appName)
    print("You are working with", cores, "core(s)")
    
    airbnb = spark.read.csv('nyc_air_bnb.csv',inferSchema=True,header=True)
    
    #modes : append, error, ignore, overwrite
    airbnb.write.mode("error").json("theairbnb")
    

    spark.stop()