# Operations on SQL DataFrames

Creating a dataframe
df= spark.read.format("csv").option("header","true").load("hdfs://localhost:9000/Spark/test.csv")

To see the content of dataframe
df.show()

Command to fetch name-column
df.select("Name").show()

Age Filter
df.filter("Age > 46").show()

groupBy method 
df.groupBy("Age").count().show()

Creating a view Table
df.createOrReplaceTempView("titanic")
A temporary view will be created by the name of the titanic, and a spark.sql will be applied on top of it to convert it into a data frame

Query on view created
sqldf = spark.sql("select * from titanic")
sqldf.show()

sqldf = spark.sql("select * from titanic where `Sex`='male'")
sqldf.show()

To count the data
sqldf.count()

To print schema on dataframe
df.printSchema()

To describe 
df.describe().show()

To get first row
df.head()

To create a new column
df.withColumn('New Age',df['Age']).show()

To rename a column
df.withColumnRenamed('Age','Age1').show()






