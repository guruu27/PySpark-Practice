from pyspark import SparkContext

sc = SparkContext('local')

def wordcount(rdd):
    res =  rdd.flatMap(lambda x: x.split())
    count_ = res.reduceByKey(lambda x,y: x+y)
    return count_


rdd1 = sc.textFile('file:///C:/spark_/book.txt')
r = wordcount(rdd1)

print(r.collect())