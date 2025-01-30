from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import StringType, IntegerType, LongType, StructType, StructField
import codecs

spark = SparkSession.builder.appName('Gt').getOrCreate()

def loadmovies():
    movienames ={}
    with codecs.open("C:/spark_/ml-100k/u.item","r",encoding="ISO-8859-1",errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movienames[int(fields[0])] = fields[1]
    return movienames

namesDict = spark.sparkContext.broadcast(loadmovies())



schema = StructType([\
    StructField('userID', IntegerType(), True),\
    StructField('movieID', IntegerType(), True),\
    StructField('rating', IntegerType(), True),\
    StructField('TimeStamp', IntegerType(), True)
])

moviesDF = spark.read.option('sep', '\t').schema(schema).csv("file:///C:/spark_/ml-100k/u.data")

movies = moviesDF.select('movieID')

popular = movies.groupBy('movieID').count()

def lookuptitles(movieID):
    return namesDict.value[movieID]

lookupudf = fn.udf(lookuptitles)

movieswithnames = popular.withColumn("movietitle", lookupudf(fn.col("movieID")))

movieswithnames.orderBy(fn.desc('count')).show()

spark.stop()