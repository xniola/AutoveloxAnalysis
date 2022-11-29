
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField
from pyspark.sql.functions import concat, count,  col,  from_csv,  lit, count, last, first, struct, udf

spark = SparkSession \
    .builder \
    .appName("DfAggregated") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')


# Stream
dfstream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","autovelox") \
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


df_aggregated = dfstream.join(df_tratti, (dfstream.varco == df_tratti.ingresso ) | (dfstream.varco == df_tratti.uscita), 'left') \
    .groupBy(['targa', 'ingresso', 'uscita']) \
    .agg(
        first('timestamp').alias('primo_timestamp'), 
        last('timestamp').alias('ultimo_timestamp'),
        count('timestamp').alias('count') 
    ).na.drop()



def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id)))\
        .write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "df_aggregated") \
        .save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = df_aggregated \
    .select(concat(
        "targa", lit(","),
        "ingresso", lit(","),
        "uscita", lit(","),
        "primo_timestamp", lit(","),
        "ultimo_timestamp", lit(","),
        "count").alias("value")) \
    .writeStream \
    .foreachBatch(foreach_batch_id) \
    .outputMode("complete") \
    .start()

query.awaitTermination()