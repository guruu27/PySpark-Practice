import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
# Set log level to WARN
sc.setLogLevel("WARN")

def normalize(txt):
    return re.compile(r'\W+', re.UNICODE).split(txt.lower())

input = sc.textFile("file:///C:/spark_/book.txt")
words = input.flatMap(normalize)
wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)
sortedk = wordCounts.map(lambda x:(x[1],x[0])).sortByKey()
res = sortedk.collect()

for r in res:
    c = r[0]
    cleanWord = r[1].encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(c))
