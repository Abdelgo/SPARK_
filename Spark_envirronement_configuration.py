import findspark
findspark.init()

import pyspark # only run after findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split

# May take awhile locally

if __name__ == "__main__":
    # spark session definition
    spark = SparkSession.builder\
                .config('spark.myprop.test', 'ok')\
                .master("local[*]")\
                .appName("Sparkstream")\
                .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    spark.conf.set("spark.myprop.path", "/CAPITAL/SPARK")
    
    print(spark.conf.get('spark.myprop.suffix'))
    print(spark.conf.get('spark.myprop.test'))
    print(spark.conf.get('spark.myprop.path'))
    

    spark.stop()

# associated with the command line :
#spark-submit --conf spark.myprop.suffix=parquet Spark_envirronement_configuration.py