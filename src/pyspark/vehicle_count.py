from pyspark.sql import SparkSession
from pyspark.sql.functions import col,  from_csv
from pyspark.sql.functions import concat,  col,  from_csv, lit

spark = SparkSession \
    .builder \
    .appName("VehiclesCount") \
    .getOrCreate() 

spark.sparkContext.setLogLevel('WARN')


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


def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id)))\
        .write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "conteggio_veicoli") \
        .save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = conteggio\
    .select(concat(
        "tratto", lit(","),
        "count"
        ).alias("value")) \
    .writeStream \
    .foreachBatch(foreach_batch_id) \
    .outputMode("complete") \
    .start()

query.awaitTermination()