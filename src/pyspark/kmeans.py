
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType
from pyspark.sql.functions import concat, count,  col,  from_csv,  lit, count, mean, first
import pyspark.sql.functions as F
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.clustering import KMeans


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


schema = "targa INT,ingresso INT, uscita INT, lunghezza DOUBLE, velocita DOUBLE, tempo_impiegato_min INT, velocita_media_per_tratto DOUBLE, tempo_medio_per_tratto_min DOUBLE"

dfstream = dfstream.selectExpr("CAST(value AS STRING)") \
    .select(from_csv(col("value"), schema, options)
            .alias("data")) \
    .select("data.*")




def foreach_batch_id(df, epoch_id):
    vecAssembler = VectorAssembler(inputCols=["velocita","tempo_impiegato_min","lunghezza"], outputCol="livello_pericolo")
    new_df = vecAssembler.transform(df)

    kmeans = KMeans(k=3, seed=1,featuresCol="livello_pericolo")  # 3 clusters
    model = kmeans.fit(new_df.select(["livello_pericolo"]))
            
    transformed = model.transform(new_df)
    transformed = transformed.groupBy('prediction').agg(mean('velocita'), mean('tempo_impiegato (min)'))

    df.withColumn("key", lit(str(epoch_id)))\
        .write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "kMeans") \
        .save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = dfstream \
    .select(concat(
        "targa", lit(","),
        "ingresso", lit(","),
        "uscita", lit(","),
        "lunghezza", lit(","),
        "velocita", lit(","),
        "tempo_impiegato_min", lit(","),
        "velocita_media_per_tratto",lit(","),
        "tempo_medio_per_tratto_min").alias("value")) \
    .writeStream \
    .foreachBatch(foreach_batch_id) \
    .outputMode("complete") \
    .start()

query.awaitTermination()
