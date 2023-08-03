from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

#True means the value can be Null
schema = StructType([ \
                     StructField("stationID", StringType(), True), \
                     StructField("date", IntegerType(), True), \
                     StructField("measure_type", StringType(), True), \
                     StructField("temperature", FloatType(), True)])

# // Read the file as dataframe
df = spark.read.schema(schema).csv("file:///SparkCourse/1800.csv")
df.printSchema()

# Filter out all but TMIN entries
minTemps = df.filter(df.measure_type == "TMIN")

# Select only stationID and temperature
stationTemps = minTemps.select("stationID", "temperature")

# Aggregate to find minimum temperature for every station
# When new transformation like min is applied we should give alias with column name if not the column will be named after the transformation command name here below column named min("temperature") will be created

minTempsByStation = stationTemps.groupBy("stationID").min("temperature")
# minTempsByStation.show()


# withColumn(): The withColumn() operation is applied first. It adds a new column named "temperature" to the minTempsByStation DataFrame. The values in this column are calculated by applying the expression func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2) to each row. This expression converts the temperature from Celsius to Fahrenheit and rounds the result to two decimal places.

# Convert temperature to fahrenheit and sort the dataset
# withcolumn creates a new column with a given name
minTempsByStationF = minTempsByStation.withColumn("temperature",
                                                  func.round(func.col("min(temperature)") * 0.1 * (9.0 / 5.0) + 32.0, 2))\
                                                  .select("stationID", "temperature").sort("temperature")

# Collect, format, and print the results
results = minTempsByStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))

spark.stop()

