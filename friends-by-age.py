from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

lines = sc.textFile("file:///C:/spark_/fakefriends.csv")
rdd = lines.map(parseLine)

'''sc.textFile(...): Reads the input file (fakefriends.csv) into an RDD (Resilient Distributed Dataset).
lines: RDD containing each line of the file as a string.
.map(parseLine): Transforms each line into a tuple (age, numFriends) using the parseLine function.'''

totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

'''.mapValues(lambda x: (x, 1)): Converts each (age, numFriends) pair into (age, (numFriends, 1)), where 1 represents a count for averaging later.
Example: (25, 100) → (25, (100, 1)).
.reduceByKey(...): Aggregates values by key (age) by summing up:
Total number of friends (x + y).
Total count of occurrences (x + y).
Example: For age 25 with two records (25, (100, 1)) and (25, (200, 1)), it computes:
Total friends: 
100
+
200
=
300
100+200=300.
Count: 
1
+
1
=
2
1+1=2.
Result: (25, (300, 2)).'''

averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

'''.mapValues(lambda x: x / x): Computes the average number of friends for each age by dividing total friends (x) by the count (x).
Example: For (25, (300, 2)), it calculates 
300/2=150
300/2=150, resulting in (25, 150).'''

results = averagesByAge.collect()

for result in results:
    print(result)
