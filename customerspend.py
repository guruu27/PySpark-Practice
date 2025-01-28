from pyspark import SparkContext, SparkConf

conf = SparkConf().setMaster('local')
sc = SparkContext(conf=conf)

def func(l):
    t = l.split(',')
    c_id, cost = t[0],t[2]
    return (int(c_id),float(cost))
    



rdd = sc.textFile('file:///C:/spark_/customer-orders.csv')
res = rdd.map(func)
res1 = res.reduceByKey(lambda x,y:x+y)
r = res1.sortBy(lambda x:x[1])

print(r.collect())
