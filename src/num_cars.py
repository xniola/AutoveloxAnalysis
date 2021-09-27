
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql.functions import split, col, from_json, from_csv, udf
from pyspark.sql.functions import lit

spark = SparkSession \
    .builder \
    .appName("CarCount") \
    .getOrCreate()

options = {'sep': ';'}
schema = "targa INT, varco INT, corsia DOUBLE, timestamp TIMESTAMP, nazione STRING"

# Stream

dfstream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.11:9092") \
    .option("subscribe", "quickstart-events") \
    .load()


dfstream = dfstream.selectExpr("CAST(value AS STRING)")
datastream = dfstream.select(
    from_csv(col("value"), schema, options).alias("data")).select("data.*")



num_cars = datastream.groupby("varco").count()

""" num_cars.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start() \
    .awaitTermination() """

# Immette i tratti autostradali
tratti = [(1, 27, 9), (2, 9, 26), (3, 26, 10), (4, 10, 18), (5, 18, 23),
          (6, 23, 15), (7, 15, 5), (8, 5, 8), (9, 8, 3), (10, 3, 13)]
tratti_schema = StructType([
    StructField('tratto', IntegerType()),
    StructField('entrata', IntegerType()),
    StructField('uscita', IntegerType())
])

df_tratti = spark.createDataFrame(data=tratti, schema=tratti_schema).cache()



conteggio_entrate = datastream.join(
    df_tratti, datastream.varco == df_tratti.entrata, 'inner') 

conteggio_entrate = conteggio_entrate.withColumn("count",lit(1))

query = conteggio_entrate.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() 

query.awaitTermination()

query.stop()

conteggio_uscite = datastream.join(
    df_tratti, datastream.varco == df_tratti.uscita, 'inner') 
conteggio_uscite = conteggio_uscite.withColumn("count",lit(-1))

   # .select('id', 'uscita').withColumnRenamed('id','id_uscita')

query = conteggio_uscite.writeStream \
    .format("console") \
    .outputMode("append") \
    .start() 

query.awaitTermination()

query.stop()

conteggio = conteggio_entrate.union(conteggio_uscite)
conteggio = conteggio.groupby("tratto").agg({'count': 'sum'})

query = conteggio.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start() 

query.awaitTermination()

query.stop()

