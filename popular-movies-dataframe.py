from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import StringType, IntegerType, LongType, StructType, StructField

spark = SparkSession.builder.appName('Gt').getOrCreate()

schema = StructType([\
    StructField('userID', IntegerType(), True),\
    StructField('movieID', IntegerType(), True),\
    StructField('rating', IntegerType(), True),\
    StructField('TimeStamp', IntegerType(), True)
])

moviesDF = spark.read.option('sep', '\t').schema(schema).csv("file:///C:/spark_/ml-100k/u.data")

movies = moviesDF.select('movieID')

popular = movies.groupBy('movieID').count().orderBy(fn.desc('count')) 

popular.show()

spark.stop()