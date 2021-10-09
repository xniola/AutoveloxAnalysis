from pyspark.sql import SparkSession
from pyspark.sql.functions import col, countDistinct,  from_csv,  window

spark = SparkSession \
    .builder \
    .appName("VehiclesCount") \
    .getOrCreate()


# Stream
dfstream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ultimi-avvistamenti") \
    .load()

options = {'sep': ','}
schema = "targa INT, timestamp TIMESTAMP, tratto INT, ingresso INT, avvistamenti INT, nazione STRING"

ultimi_avvistamenti = dfstream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .select(from_csv(col("value"), schema, options)
            .alias("data"), col("key").alias("batch")) \
    .select("batch", "data.*")

conteggio = ultimi_avvistamenti \
    .groupBy("batch", "tratto") \
    .count() \
    .orderBy("batch", ascending=False)

# Output
print("\n\n\nStarting...\n\n\n")
query = conteggio.writeStream \
    .format("console") \
    .option("numRows", 200) \
    .option("truncate", False) \
    .outputMode("complete") \
    .start()

query.awaitTermination()

query.stop()
