from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0 #CELCIUS TO FARANHEIT
    return (stationID, entryType, temperature)

# data format
# LOCATION ID, DATE, (TEMP MAX OR TEMP MIN), TEMPERATURE, .......

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)
# print(parsedLines.collect())

minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
# print(minTemps.collect())

stationTemps = minTemps.map(lambda x: (x[0], x[2]))
# print(stationTemps.collect())

# reduceByKey is a transformation operation in Spark that groups the data by keys and applies a function to the values of each group. In this case, it groups the temperature data by keys (station identifiers) and applies the provided lambda function to the values.
#
# The lambda function lambda x, y: min(x, y) takes two values x and y and returns the minimum value between them. It compares the temperatures for each station and keeps the lower value.
#
# By applying reduceByKey(lambda x, y: min(x, y)), the RDD is transformed into a new RDD, where each key (station identifier) is associated with the minimum temperature value among all the temperatures for that station.

minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
# print(minTemps.collect())

results = minTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
