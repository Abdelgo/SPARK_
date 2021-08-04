import findspark
findspark.init()

import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import window
from pyspark.sql.types import StructType, IntegerType, TimestampType

# May take awhile locally

if __name__ == "__main__":
    # spark session definition
    spark = SparkSession.builder.master("local[*]").appName("Sparkstream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # spark session info
    cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
    print(spark.sparkContext.appName)
    print("You are working with", cores, "core(s)")
    
    # record schema
    record_schema = StructType().add("guid", "string").add("timestamp", "timestamp").add("val","integer")
    
    
    # stream input
    streamDf = spark.readStream.option("sep","\t").schema(record_schema).csv("event-time-data")
    print(streamDf.printSchema())

    print(streamDf.isStreaming)
    # to lunch the socket lunch from terminal the command
    # cat lorem.txt | nc -l 8088
    
    #deduplication 
    streamDf.withWatermark("timestamp","20 seconds") # loog only for duplicates in the last 20s
    streamDf.dropDuplicates()
    
    # creat the window
    windowDf = streamDf.groupby(window("timestamp", "5 seconds")).sum("val")
    
    # streaming query
    query = windowDf.writeStream.outputMode("complete").format("console").start()
                    #append/complete etc console:to print result on the screen
    
    query.awaitTermination()

    spark.stop()