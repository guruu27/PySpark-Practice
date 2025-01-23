from pyspark import SparkContext

sc = SparkContext("local", "Example")
rdd = sc.parallelize([1, 2, 3, 4, 5])
print(rdd.collect())  # Output: [1, 2, 3, 4, 5]