from pyspark.sql import SparkSession
from pyspark.sql.functions import concat,col,  from_csv,  lit, mean
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("kMeans") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')


# Stream
dfstream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","dd1") \
    .load()

options = {'sep': ','}
schema = "targa INT, ingresso INT, uscita INT, lunghezza DOUBLE, velocita DOUBLE, tempo_impiegato_min DOUBLE, velocita_media_per_tratto DOUBLE, tempo_medio_per_tratto_min DOUBLE"

dfstream = dfstream.selectExpr("CAST(value AS STRING)") \
    .select(from_csv(col("value"), schema, options)
            .alias("data")) \
    .select("data.*")



df_clustering = dfstream.withColumn('prediction', 
     F.when(col('velocita_media_per_tratto') <= 90, 1) \
    .when((col('velocita_media_per_tratto') > 90) & (col('velocita_media_per_tratto') < 120), 2) \
    .otherwise(3)) \
    .groupBy('prediction').agg(mean('velocita_media_per_tratto').alias("velocita_media"), mean('tempo_medio_per_tratto_min').alias("tempo_medio_min"))


def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id)))\
        .write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "clusterigno") \
        .save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = df_clustering \
    .select(concat(
        "prediction", lit(","),
        "velocita_media", lit(","),
        "tempo_medio_min")) \
    .writeStream \
    .foreachBatch(foreach_batch_id) \
    .outputMode("complete") \
    .start()

query.awaitTermination()
