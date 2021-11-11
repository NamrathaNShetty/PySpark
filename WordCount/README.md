# Word Count Program

Hadoop version - 3.3.1
Spark Version - 3.1.2

Start hadoop daemons
$ start-all.sh

Start spark daemons
./sbin/start-all.sh

Open pyspark
$ pyspark

Load a text file on which we want to perform word count
book = sc.textFile("hdfs://localhost:9000/Spark/book.txt")

Using flatMap and lambda function to split line
words = book.flatMap(lambda line:line.split(" "))

Map the words and count
wordcount = words.map(lambda word:(word,1)).reduceByKey(lambda a,b:a+b)



