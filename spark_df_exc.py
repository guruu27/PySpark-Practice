from pyspark.sql import SparkSession, functions as func


spark = SparkSession.builder.appName("S").getOrCreate()

people = spark.read.option("header",'true').option('inferschema','true').csv("file:///C:/spark_/fakefriends-header.csv")

people.select('*').show()

people.groupBy('age').agg(func.round(func.avg('friends'),2).alias("friends_avg")).sort('age').show()

spark.stop()