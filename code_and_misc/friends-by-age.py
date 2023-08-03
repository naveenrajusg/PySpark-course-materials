from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)


#data set - userid,name,age, number of friends
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
rdd = lines.map(parseLine)

# The rdd.collect() operation prints the content of the RDD rdd, which contains the parsed data from the "fakefriends.csv" file. Each element in the RDD is a tuple representing the age and the number of friends.
#
# The totalsByAge.collect() operation prints the content of the RDD totalsByAge, which is obtained by performing transformations on the rdd. The RDD totalsByAge contains tuples where the key is the age, and the value is a tuple representing the sum of friends and the count of occurrences for that age.


print(rdd.collect())

# mapValues is a transformation function in Spark that applies a provided function to the values of each key-value pair in an RDD, while keeping the keys unchanged.
#
# In this case, the lambda function lambda x: (x, 1) takes a value x and returns a tuple (x, 1). It wraps the original value x in a tuple and adds a count of 1 as the second element of the tuple. This operation is commonly used for aggregations and counting purposes.
#
# By applying mapValues(lambda x: (x, 1)), the RDD is transformed into a new RDD where the values are now tuples, with the original value as the first element and a count of 1 as the second element.
#
# For example, let's consider an input RDD rdd with the following key-value pairs:
#
# [('A', 10), ('B', 5), ('C', 7)]
# After applying
# rdd.mapValues(lambda x: (x, 1)),
# the resulting RDD would be:
#
# [('A', (10, 1)), ('B', (5, 1)), ('C', (7, 1))]

ww = rdd.mapValues(lambda x: (x, 1))
print(ww.collect())


# The code ww.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])) is a transformation operation applied to an RDD ww. Let's break down how this operation works:
#
# reduceByKey is a transformation function in Spark that groups the data by keys and applies a provided function to the values of each group. In this case, it groups the data by keys and applies the lambda function to the values.
#
# The lambda function lambda x, y: (x[0] + y[0], x[1] + y[1]) takes two values x and y and performs element-wise addition on them. It assumes that x and y are tuples with two elements each.
#
# The lambda function adds the first elements of x and y to calculate the first element of the resulting tuple and adds the second elements of x and y to calculate the second element of the resulting tuple.
#
# By applying reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])), the RDD is transformed into a new RDD where the values are tuples obtained by aggregating the values for each key using element-wise addition.

# (x[0],x[1]) and (y[0],y[1])
# For example, let's consider an input RDD ww with the following key-value pairs:
# [('A', (10, 2)), ('B', (5, 1)), ('A', (3, 1)), ('B', (2, 2))]
#
# After applying ww.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1])), the resulting RDD would be:
# [('A', (13, 3)), ('B', (7, 3))]

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
print(totalsByAge.collect())

# # The totalsByAge.mapValues(lambda x: x[0] / x[1]) operation applies a transformation to the totalsByAge RDD. It takes each value x, which is a tuple representing the total number of friends and the count for a specific age, and calculates the average by dividing the total number of friends x[0] by the count x[1].
#
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])
results = averagesByAge.collect()
print("**********************************")
for result in results:
    print(result)
