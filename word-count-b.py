import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalize(txt):
    return re.compile(r'\W+', re.UNICODE).split(txt.lower())

input = sc.textFile("file:///C:/spark_/book.txt")
words = input.flatMap(normalize)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
