from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

#
# When you provide a file path in the format file:///, it indicates that the file is located on the local file system of the machine running the code. In this case, the file system's default behavior is to search for the file in the current working directory.
names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt")

# by default the column name will be value if not specified
lines = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.

#func.trim(): It trims leading and trailing whitespace characters from the values in the column. This ensures that any extra spaces are removed before further processing.
#func.split(func.trim(func.col("value")), " "): It splits the trimmed column values using space (" ") as the delimiter. The result is an array of substrings obtained by splitting the value on spaces.
#[0]: It retrieves the first element from the resulting array. This assumes that you are interested in the first substring after splitting the value on spaces.

connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# The sort() function in PySpark is used to sort a DataFrame based on one or more columns. By default, the sorting is done in ascending order. However, by applying the desc() function to the column, you can specify that you want the sorting to be done in descending order.
mostPopular = connections.sort(func.col("connections").desc()).first()

print(mostPopular)

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

