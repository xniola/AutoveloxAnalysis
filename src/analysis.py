#from pyspark.sql import SparkSession
#from pyspark.sql.functions import explode
#from pyspark.sql.functions import split
#from pyspark import spark

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from pyspark.sql.functions import col


sc = SparkContext('local')
spark = SparkSession(sc)
#spark.sparkContext.setLogLevel("ERROR")


# Mi iscrivo al topic di kafka
csvDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "autovelox") \
    .load()  

# Estraggo i dati del csv
dati = csvDF.select(col("value").cast("string")).alias("csv").select("csv.*")

# Splitto i dati in colonne (inferisco un dataframe)
df = dati \
      .selectExpr("split(value,',')[0] as targa" \
                 ,"split(value,',')[1] as varco" \
                 ,"split(value,',')[2] as corsia" \
                 ,"split(value,',')[3] as timestamp" \
                 ,"split(value,',')[4] as nazione"
                 )


# Query del dataframe appena inferito

query = df     \
  .writeStream  \
  .format("console") \
  .outputMode("append") \
  .start()

print(type(query))
query.awaitTermination()

'''
query2 = df.select('varco').groupby().sum()\
  .writeStream\
  .format("console")\
  .outputMode("append")\
  .start()\
  .awaitTermination()
'''
'''
query1 = df.collect.foreach(print)\
  .writeStream\
  .format("console")\
  .start()

query1.awaitTermination()
'''

