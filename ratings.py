from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)
# Set log level to WARN
sc.setLogLevel("WARN")

lines = sc.textFile("file:///C:/spark_/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for k, v in sortedResults.items():
    print("%s %i" % (k, v))
