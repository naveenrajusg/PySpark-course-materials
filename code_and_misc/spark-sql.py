from pyspark.sql import SparkSession
from pyspark.sql import Row

# The Row class represents a row of data in a DataFrame. It provides a convenient way to work with structured data in Spark. Like creating rows in a dataframe and accessing rows of it.

# The line from pyspark.sql import SparkSession is used to import the SparkSession class from the pyspark.sql module in Spark.
# SparkSession is the entry point to programming with DataFrame and SQL in Spark. It provides a way to interact with structured data using the DataFrame API and perform various data manipulation and analysis operations.


# The purpose of encoding the name string as UTF-8 might be to handle special characters or ensure consistent encoding when working with non-ASCII characters.
# Encoding as UTF-8 is a common practice in data processing, especially when dealing with text data that may include non-ASCII characters. It ensures that the string is represented in a standardized format that can handle a wide range of characters and maintain compatibility across different systems and applications.


# Create a SparkSession
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

# Infer the schema, and register the DataFrame as a table.
schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView("people")

# SQL can be run over DataFrames that have been registered as a table.
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")

# The results of SQL queries are RDDs and support all the normal RDD operations.
for teen in teenagers.collect():
  print(teen)

# We can also use functions instead of SQL queries:
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()


