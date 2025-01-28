from pyspark.sql import SparkSession
from pyspark.sql import functions as fn
from pyspark.sql.types import StructType, StringType, IntegerType, VarcharType, FloatType, StructField

spark = SparkSession.builder.master("local").appName("CustomerSpend").getOrCreate()

schema = StructType([\
    StructField('C_id', IntegerType(), True),\
    StructField('O_id', IntegerType(), True),\
    StructField('spend', FloatType(), True)\
]
)

df = spark.read.schema(schema).csv("file:///C:/spark_/customer-orders.csv")

df1 = df.groupBy('C_id').sum('spend')

df1.withColumn('spend', fn.round(fn.col('sum(spend)'),2)).select('C_id','spend').sort('spend')

res = df1.collect()

for r in res:
    print("Customer ID", r[0])
    print("spend", r[1])

spark.stop()


