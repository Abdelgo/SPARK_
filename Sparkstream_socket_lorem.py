import findspark
findspark.init()

import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# May take awhile locally

if __name__ == "__main__":
    # spark session definition
    spark = SparkSession.builder.master("local[*]").appName("Sparkstream").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # spark session info
    cores = spark._jsc.sc().getExecutorMemoryStatus().keySet().size()
    print(spark.sparkContext.appName)
    print("You are working with", cores, "core(s)")
    
    
    # stream input
    streamtext = spark.readStream.format("socket").option("host","localhost").\
                            option("port",8088).load()
    print(streamtext.isStreaming)
    # to lunch the socket lunch from terminal the command
    # cat lorem.txt | nc -l 8088
    
    wordsdf = streamtext.select(explode(split(streamtext.value," ")).alias("word"))
    print(wordsdf)
    # wordsdf.show() # cannot be done because we are in streaming
    
    # streaming query
    query = wordsdf.writeStream.outputMode("append").format("console").start()
                    #append/complete etc console:to print result on the screen
    
    query.awaitTermination()

    spark.stop()