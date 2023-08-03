from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostObscureSuperheroes").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("file:///SparkCourse/Marvel-names.txt")

lines = spark.read.text("file:///SparkCourse/Marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim whitespace from each line as this
# could throw the counts off by one.
# by default the column name will be value if not specified
connections = lines.withColumn("id", func.split(func.trim(func.col("value")), " ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value")), " ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

# print(connections.collect())


minConnectionCount = connections.agg(func.min("connections")).first()[0]


minConnections = connections.filter(func.col("connections") == minConnectionCount)

# In the above code, join() is used to perform an inner join between the minConnections and names DataFrames. The "id" column is specified as the join key.
# The INNER JOIN keyword selects records that have matching values in both tables.
minConnectionsWithNames = minConnections.join(names, "id")


print("The following characters have only " + str(minConnectionCount) + " connection(s):")

minConnectionsWithNames.select("name").show()