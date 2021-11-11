# BASIC OPERATIONS WITH PYSPARK

Hadoop version - 3.3.1
Spark Version - 3.1.2

First start all hadoop daemons
$ start-all.sh

Go inside spark home directory
$ cd $SPARK_HOME

Start spark daemons
$ ./bin/start-all.sh

Now start pyspark
$ pyspark
This will redirect you to jupyter notebook in browser

Load the file from HDFS
book =  sc.textFile("hdfs://localhost:9000/Spark/book.txt")

To observe the first five lines from loaded file
book.take(5)

To show all the data
book.collect()

To count the lines 
 book.count()
 
Increasing the number of partitions
book1 = book.repartition(11)
book1.getNumPartitions()

Decreasing the number of partitions
Book2 =book1.coalesce(10)
book2.getNumPartitions()

To get first line
book2.first()

Removing a specific line:
words=[ “May I come in”]
book3 = book2.filter(lambda line:line not in words)





