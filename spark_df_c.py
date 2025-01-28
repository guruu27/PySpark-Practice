from pyspark.sql import SparkSession, functions as fn

spark = SparkSession.builder.appName("S").getOrCreate()

words = spark.read.text("file:///C:/spark_/book.txt")

word = words.select(fn.explode(fn.split(words.value,"\\W+")).alias("w"))
wordswoempt = word.filter(word.w!="")

lowerc = wordswoempt.select(fn.lower(wordswoempt.w).alias('wo'))

wordc = lowerc.groupBy('wo').count()

wordc.show()