from pyspark import SparkContext

sc = SparkContext('local')

def parselines(l):
    f = l.split(',')
    loc = f[0]
    entype = f[2]
    temp = float(f[3]) * 0.1 * (9.0/5.0) + 32.0
    return (loc,entype,temp)

lines = sc.textFile('file:///C:/spark_/1800.csv')
rdd = lines.map(parselines)

res1 = rdd.filter(lambda x: 'TMAX' in x[1])
res = res1.reduceByKey(lambda x,y,z: min(x,y,z))
print(res.collect())