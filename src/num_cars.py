
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType, StructType, StructField
from pyspark.sql.types import StringType, IntegerType, ArrayType
from pyspark.sql.functions import split, col, from_json, from_csv, udf
from pyspark.sql.functions import lit

spark = SparkSession \
    .builder \
    .appName("CarCount") \
    .getOrCreate()


# Stream

dfstream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "192.168.1.11:9092") \
    .option("subscribe", "quickstart-events") \
    .load()

options = {'sep': ';'}
schema = "targa INT, varco INT, corsia DOUBLE, timestamp TIMESTAMP, nazione STRING"

dfstream = dfstream.selectExpr("CAST(value AS STRING)")
datastream = dfstream.select(
    from_csv(col("value"), schema, options).alias("data")).select("data.*")



# Immette i tratti autostradali
tratti = [(1, 27, 9), (2, 9, 26), (3, 26, 10), (4, 10, 18), (5, 18, 23),
          (6, 23, 15), (7, 15, 5), (8, 5, 8), (9, 8, 3), (10, 3, 13)]
tratti_schema = StructType([
    StructField('tratto', IntegerType()),
    StructField('entrata', IntegerType()),
    StructField('uscita', IntegerType())
])

df_tratti = spark.createDataFrame(data=tratti, schema=tratti_schema).cache()

# ultimi avvistamenti:  i record vengono ordinati per targa e viene mantenuto 
#                       solo l'ultimo timestamp e tratto autostradale in cui 
#                       si trova

ultimi_avvistamenti = datastream.join(
    df_tratti, datastream.varco == df_tratti.entrata, 'inner')
ultimi_avvistamenti.groupBy("targa").agg({"timestamp":"max"})


conteggio_entrate = datastream.join(
    df_tratti, datastream.varco == df_tratti.entrata, 'inner') 

conteggio_entrate = conteggio_entrate.withColumn("count",lit(1))


conteggio_uscite = datastream.join(
    df_tratti, datastream.varco == df_tratti.uscita, 'inner') 
conteggio_uscite = conteggio_uscite.withColumn("count",lit(-1))


conteggio = conteggio_entrate.union(conteggio_uscite)
conteggio = conteggio.groupby("tratto").agg({'count': 'sum'})

