from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions as func

# getOrCreate() is a method of the Builder class that either retrieves an existing SparkSession or creates a new one if it doesn't already exist. This ensures that only one SparkSession is created per application.

spark = SparkSession.builder.appName("FriendsByAge").getOrCreate()

lines = spark.read.option("header", "true").option("inferSchema", "true").csv("file:///SparkCourse/fakefriends-header.csv")

# Select only age and numFriends columns
friendsByAge = lines.select("age", "friends")

# From friendsByAge we group by "age" and then compute average
friendsByAge.groupBy("age").avg("friends").show()

# Sorted
friendsByAge.groupBy("age").avg("friends").sort("age").show()

#The agg function is typically used in combination with other functions from pyspark.sql.functions or as part of a SQL expression.
# Formatted more nicely
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)).sort("age").show()

# With a custom column name
friendsByAge.groupBy("age").agg(func.round(func.avg("friends"), 2)
  .alias("friends_avg")).sort("age").show()

spark.stop()
