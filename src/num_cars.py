
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructType, StructField
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import concat, split, col, from_csv, struct, to_json
from pyspark.sql.functions import lit

spark = SparkSession \
    .builder \
    .appName("CarCount") \
    .getOrCreate()


# Stream
dfstream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "autovelox") \
    .load()

dfstream = dfstream.selectExpr("CAST(value AS STRING)") \
    .select(from_csv(col("value"), schema, options)
            .alias("data")) \
    .select("data.*")

in_stream = dfstream \
    .select(col("targa"), col("varco"), col("corsia"), col("timestamp"), col("nazione")) \
    .withColumnRenamed("targa", "in_targa") \
    .withColumnRenamed("timestamp", "in_timestamp") \
    .withWatermark("in_timestamp", "3 minutes")


# Immette i tratti autostradali
tratti = [(1, 27, 9), (2, 9, 26), (3, 26, 10), (4, 10, 18), (5, 18, 23),
          (6, 23, 15), (7, 15, 5), (8, 5, 8), (9, 8, 3), (10, 3, 13),
          (11, 22, 1), (12, 1, 12), (13, 12, 25), (14, 25, 20), (15, 20, 2),
          (16, 2, 16), (17, 16, 4), (18, 4, 21)]
tratti_schema = StructType([
    StructField('tratto', IntegerType()),
    StructField('entrata', IntegerType()),
    StructField('uscita', IntegerType())
])

df_tratti = spark.createDataFrame(data=tratti, schema=tratti_schema).cache()

# ultimi avvistamenti:  i record vengono ordinati per targa e viene mantenuto
#                       solo l'ultimo timestamp e tratto autostradale in cui
#                       si trova

ultimi_avvistamenti = dfstream.join(
    df_tratti, dfstream.varco == df_tratti.entrata, 'inner') \
    .groupBy("targa") \
    .agg(max("timestamp"), count(lit(1)), first("tratto")) \
    .orderBy('count(1)', ascending=False)
# .withWatermark("timestamp", "3 minutes") \
# .orderBy('timestamp', ascending=False)
conteggio = ultimi_avvistamenti.groupBy("tratto").count()
# stream-stream join


sink = conteggio.select(concat("tratto", lit(" "), "sum(count)", lit(" ")).alias("value")) \
.writeStream \
.format("kafka")\
.outputMode("update") \
.option("kafka.bootstrap.servers", "localhost:9092")\
.option("topic", "risultati")\
.option("checkpointLocation", "kafka_sink")\
.start()

sink.awaitTermination()


'''
query = sink = conteggio.select(concat("tratto", lit(" "), "sum(count)", lit(" ")).alias("value")) \
.writeStream \
.format("console")\
.outputMode("update") \
.start()

query.awaitTermination()
'''
