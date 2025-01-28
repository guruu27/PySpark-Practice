from pyspark import SparkContext

sc = SparkContext('local')

def wordcount(rdd):
    res =  rdd.flatMap(lambda x: x.split())
    res1 = res.map(lambda x: (x,1))
    count_ = res1.reduceByKey(lambda x,y: x+y)
    return count_


rdd1 = sc.textFile('file:///C:/spark_/book.txt')
r = wordcount(rdd1)

for w,c in r.collect():
    cw = w.encode('ascii','ignore')
    if cw:
        print(cw,c)
