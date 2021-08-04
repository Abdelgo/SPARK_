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
    
    
    

    spark.stop()