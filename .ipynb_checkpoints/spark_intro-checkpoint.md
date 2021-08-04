Spark installation:
open a shell on mac :
> javac - version (to chek if 1.8)
> wget http:thelinkoftheapachespark
> tar xvf 'thefiledownloaded'
> cd into the file 
> ./sbin/start-all.sh (will start an instance of spark)
> ./bin/park-shell (run the spark shell)
check the spark UI port

inside the bin folder > ls
beeline			load-spark-env.sh	spark-class		spark-sql		sparkR
beeline.cmd		pyspark			spark-class.cmd		spark-sql.cmd		sparkR.cmd
docker-image-tool.sh	pyspark.cmd		spark-class2.cmd	spark-sql2.cmd		sparkR2.cmd
find-spark-home		pyspark2.cmd		spark-shell		spark-submit
find-spark-home.cmd	run-example		spark-shell.cmd		spark-submit.cmd
load-spark-env.cmd	run-example.cmd		spark-shell2.cmd	spark-submit2.cmd

# REST API for monitoring

> curl localhost:4040/api/v1/applications/ # this will show all the running applications (in json format)

look for jobs or else inside a specific application  
> curl localhost:4040/api/v1/applications/*appID/jobs
> curl localhost:4040/api/v1/applications/*appID/stages
> curl localhost:4040/api/v1/applications/*appID/executors
> curl localhost:4040/api/v1/applications/*appID/streaming/statistics
> curl localhost:4040/api/v1/applications/*appID/environment

# Memory allocation
spark use the JVM heap
the memory fraction total is 60%, half is executor memory and half for execution itself, this affect how often the garbage collector gets run  

Now you can specify the total fraction of JVM heap space through a conf pass with spark-shell or spark-submit    
Ex: 
> pyspark --conf spark.memory.fraction=0.75

there is another option for object caches like RDDs and Dataframes that we can pass the sorage fraction conf parameter

> pyspark --conf spark.memory.storageFraction =0.3 (for 30%)

# Tunning spark applications 
## 1. Speculation
speculation is used to speed up completion of slow tasks
speculation is turned off by default

> pyspark --conf spark.speculation=true --conf spark.speculation.interval=250 --conf spark.speculation.multiplier=1.25 --conf spark.speculation.quantile=0.8

default values:  
spark.speculation.interval = 100 (ms) (how often it checks for slow tasks)  
spark.speculation.multiplier = 1.5 (how much time slower the task must be to be considered for speculation)  
spark.speculation.quantile = 0.7 (pourcentage of tasks that need to be completed before starting speculation)  

## 2. serialization
Serialization is an important role in Performance of distributed applications  
Tuning serialization  
<img src="/photos/1.png">

the Java type is convenient but it could slow and bloated if it's done naively  
<img src="/photos/2.png">
by defult spark serialize its objects using Java's ObjectOutputStream framework, it's felxible but often slow  
> another option is Kryo serialization, it serializes objects faster, more compact than java serialization (up to 10 times)  
<img src="/photos/3.png">

## 3. Memory Tunning
Three considerations to take care of:  
> the amount of memory used by an object
> Cost of access to an object
> overhead of garbage collection
Java objects
- fast to access
- but Consume more space  
<img src="/photos/4.png">
<img src="/photos/5.png">
<img src="/photos/6.png">

## 4. Executor memory
how to set driver memory, executor memory, master and worker daemon memory
> export SPARK_SAEMON_MEMORY=2g (for 2 gigabytes)
> spark-submit(or spark-shell or pyspark) --driver-memory=2g -- executer-memory=1g --conf spark.memory.fraction=0.75 --conf spark.memory.storageFraction=0.4

those options can be then checked on web UI and see the beahvior for an eventual fine tunning  

## 5. garbage collection tunning
before starting tunning garbage collection you start by collecting some statistics, this will determine how much garbage collections occurs and for how long  

<img src="/photos/7.png">
<img src="/photos/8.png">

### Advanced tunning procedure
<img src="/photos/9.png">

## 4. Parrallelism
how to set the level of parallelism to maximize the utilisation of your cluster  
for this you should be aware of:  
> the number of cores each CPU ibn your cluster has
> good rule of thumb, that parallelism value is set to 2 to 3 per CPU core available

so if you have a 10 CPUs with 4 cores each, parallelism value will be :  
(2 x 4 x 10 = 80) to (3 x 4 x 10 = 120)  
this is the bechmark, this will affect the speed of your map, reduce and partionning operations  
to set the parallelism value:  
> spark-submit --conf spark.default.parallelism=8 file.py


## 4. Broadcast functionality
broadcast functionality is used to reduce the size of your tasks in your spark cluster
code example:  
to reduce the size of serialized task
```python
from pyspark.sql.functions import broadcast
speciestable = broadcast(spark.createDataFrame([("setosa",1),("versicolor",2),("virginica",3)],schema=schema))```  


## 5. Explain query execution

this is a feature that exists since spark 2.x  
if you are dealing with slow running query especiially joins, you can run the following code example :  

```python
irisjoin = iris.join(speciestable,on='species')
irisjoin.explain()
irisjoin.explain(extended=True)```

## 6. Compression
```python
dataframe.write.option("compression","snappy").parquet("filename-target") # snappy or gzip
```
gzip is more efficient in compression but slower during compression execution

# SPARK SECURITY
## 1. securing Web UI
we can secure the access to the Web UI via a firewall, as an example here, we will use iptables  
procedure to only allow the localhost to access the WEB UI  
open a shell and type the following command  
> sudo iptables -L (to see the rules of restricution already coded)
> sudo iptables -A INPUT -p tcp -s localhost --dport 4040 -j ACCEPT (to allow localhost to access)
> sudo iptables -A INPUT -p tcp --dport 4040 -j DROP (to deny accesss for anyone else on the network)

## 2. securing access to log file
this method is not specific to spark but any folder in a Unix system  
session> ls -la  
drwxr-xr-x  21 livai  staff      672  3 aoû 20:10 .  
drwxr-xr-x  15 livai  staff      480 29 jul 11:13 ..  
drwxr-xr-x  14 livai  staff      448  2 aoû 22:50 .ipynb_checkpoints  
-rw-r--r--   1 livai  staff     1549  2 aoû 17:36 3_spark_streaming_deduplication.py  
-rw-r--r--   1 livai  staff      439  2 aoû 17:39 3_timestamp_generator_with_uuid.py  

(-rw-r--r--) divided in 3 (user)(group)(anyone connected to the system)  
here the user has rw permission, group and other can only read files  
In order to modify this we use the chmod command, we go into the folder and then type :  
session> chmod 660 * (this will apply this rule on all the files included)
if you want to apply the rules on the folder and subfolder then type:  
session> find . -type f -exec chmod 660 {} \; (for files)  (rw-rw-r)
session> find . -type d -exec chmod 770 {} \; (for directories)

https://ftp.kh.edu.tw/Linux/Redhat/en_6.2/doc/gsg/s1-navigating-chmodnum.htm

## 3. SSL setting to secure spark communication
in this section we give the procedure to create an SSL setting:  
<img src="/photos/10.png">
<img src="/photos/11.png">
<img src="/photos/12.png">
<img src="/photos/13.png">

## 4. Shared secret
how to set a shared secret for spark authentication
this is one of the easiest way to authenticate nodes in your cluster  

the first step is to stop any activity in your cluster by the command:  
session> stop-all.sh
session> spark-submit --conf spark.authenticate=true --conf spark.authenticate.secret=secretpass helloworld.py  
when we run this command, it will make sure that the cluster, our masters and workers are using spark.authenticate=true  

if we want to set this configuration to be considered automatically then we follow:  
<img src="/photos/14.png">
<img src="/photos/15.png">
so we can use :  
session> spark-submit helloworld.py  

## 5. Yarn deployment
how to configure spark athentication on Yarn deployments 
Remark: when we set spark.authenticate=true, Yarn does the work behind the scenes to handle shared keys or secrets for our application.  
Each application in the YARN deployment is handled uniquely, so there aren't separate configurationsfor shared secrets between masters and workers like we need to set for other spark deployment  
the only thing to do is :  
sesssion>spark-submit --conf spark.authenticate=true helloworld.py

## 6. SASL Encryption
how to enabel SASL encryption for a spark application, this is done for shuffler block transfers between nodes to ensure that they are encrypted. So every node in the cluster will need to use the same setting for authentication and shared secrets along with enable SASL enryption set to true.  

to do this we follow (this is true for spark-shell or submiting any application), so for each node for each process, this configuration will ne dto be set the same.
<img src="/photos/16.png">

## 6. Network security
<img src="/photos/17.png">
<img src="/photos/18.png">
<img src="/photos/19.png">