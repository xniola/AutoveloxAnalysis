
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import concat, count,  col,  from_csv,  lit, count, max, first, struct, udf

spark = SparkSession \
    .builder \
    .appName("LastSeen") \
    .getOrCreate()


# Stream
dfstream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rilevamenti-targa") \
    .load()

options = {'sep': ','}
schema = "targa INT, varco INT, corsia DOUBLE, timestamp TIMESTAMP, nazione STRING"

dfstream = dfstream.selectExpr("CAST(value AS STRING)") \
    .select(from_csv(col("value"), schema, options)
            .alias("data")) \
    .select("data.*")


# Immette i tratti autostradali
tratti = [(1, 27, 9), (2, 9, 26), (3, 26, 10), (4, 10, 18), (5, 18, 23),
          (6, 23, 15), (7, 15, 5), (8, 5, 8), (9, 8, 3), (10, 3, 13),
          (11, 22, 1), (12, 1, 12), (13, 12, 25), (14, 25, 20), (15, 20, 2),
          (16, 2, 16), (17, 16, 4), (18, 4, 21)]

tratti_schema = StructType([
    StructField('tratto', IntegerType()),
    StructField('ingresso', IntegerType()),
    StructField('uscita', IntegerType())])

df_tratti = spark.createDataFrame(data=tratti, schema=tratti_schema).cache()


# ultimi avvistamenti:  i record vengono ordinati per targa e viene mantenuto
#                       solo l'ultimo timestamp e tratto autostradale in cui
#                       si trova
ultimi_avvistamenti = dfstream.join(
    df_tratti, dfstream.varco == df_tratti.ingresso, 'inner') \
    .groupBy("targa") \
    .agg(
        max(struct("timestamp", "tratto", "ingresso")).alias("max"),
        count(lit(1)).alias("avvistamenti"),
        first("nazione").alias("nazione")) \
    .select("targa", "max.timestamp", "max.tratto", "max.ingresso", "avvistamenti", "nazione")

""" current_time = udf(lambda: str(
    datetime.datetime.now().timestamp()), StringType()) """


def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id)))\
        .write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "ultimi-avvistamenti") \
        .save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = ultimi_avvistamenti\
    .select(concat(
        "targa", lit(","),
        "timestamp", lit(","),
        "tratto", lit(","),
        "ingresso", lit(","),
        "avvistamenti", lit(","),
        "nazione").alias("value")) \
    .writeStream \
    .foreachBatch(foreach_batch_id) \
    .outputMode("complete") \
    .start()

# Test
""" query_console = ultimi_avvistamenti\
    .select(concat(
        "targa", lit(","),
        "timestamp", lit(","),
        "tratto", lit(","),
        "ingresso", lit(","),
        "avvistamenti", lit(","),
        "nazione").alias("value")) \
    .writeStream \
    .foreachBatch(foreach_batch_id) \
    .format("console") \
    .option("numRows", 200) \
    .outputMode("complete") \
    .start()
 """

""" query_console.stop()
 """

query.awaitTermination()
