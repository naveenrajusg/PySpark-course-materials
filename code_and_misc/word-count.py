from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(lambda x: x.split())

# The countByValue() function is an action in Apache Spark that returns a Python dictionary where the keys represent the unique values in the RDD, and the values represent the count of each value. In this case, the unique words and their respective counts are returned as a dictionary.

wordCounts = words.countByValue()
# print(wordCounts.collect())

# And this just takes care of some encoding issues so in case something was encoded as UTF 8 or Unicode
#
# in our original text this makes sure that we can display it.
#
# And our terminal in our command prompt by converting it to ASCII format so it also says we're going
#
# to ignore any conversion errors that might occur in the process of trying to convert from Unicode to ASCII.
#
# So this is just a way to make sure that we can display these words without errors even though they might
#
# contain special characters just a little Python trick for you there.
#
# And if that comes back successfully we will print out each word and the number of times that occurred.

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
