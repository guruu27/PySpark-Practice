from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.option("delimiter"," ").schema(schema).csv("C:/spark_/MarvelNames")

names.show(truncate=False)

lines = spark.read.text("C:/spark_/MarvelGraph")

connections = lines.withColumn("id", func.split(func.trim(func.col("value"))," ")[0]) \
    .withColumn("connections", func.size(func.split(func.trim(func.col("value"))," ")) - 1) \
    .groupBy("id").agg(func.sum("connections").alias("connections"))

#connections.show()
    
mostPopular = connections.sort(func.col("connections").desc()).first()

print(f"Most popular superhero ID >>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>: {mostPopular[0]}, Connections: {mostPopular[1]}")

mostPopularName = names.filter(func.col("id") == mostPopular[0]).select("name").first()

print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")