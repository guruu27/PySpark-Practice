from pyspark import SparkContext, SparkConf

sc = SparkContext("local")

def parseLines(line):
    fields = line.split(',')
    Location = fields[0]
    entype = fields[2]
    temp = float(fields[3])* 0.1 * (9.0/5.0) + 32.0
    return (Location,entype,temp)
lines = sc.textFile('file:///C:/spark_/1800.csv')

res = lines.map(parseLines)

mint = res.filter(lambda x: 'TMIN' in x[1])
staiontemp = mint.map(lambda x:(x[0],x[2]))
min_stationtemp = staiontemp.reduceByKey(lambda x,y: min(x,y))


for r in min_stationtemp.collect():
    print( r[0] +" "+ '{:.2f}F'.format(r[1]))