import re
from pyspark import SparkConf, SparkContext


# The function normalizeWords uses the re module from the Python standard library to perform regular expression operations. Specifically, it uses the re.compile function to compile a regular expression pattern.
#
# The regular expression pattern r'\W+' matches any sequence of one or more non-alphanumeric characters. This pattern is used to split the text string into a list of words.
#
# The re.UNICODE flag is passed to the re.compile function to ensure that Unicode characters are properly handled during the text normalization process.

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))


