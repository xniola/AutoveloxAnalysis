
import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, StringType, StructType, StructField, DoubleType
from pyspark.sql.functions import concat, count,  col,  from_csv,  lit, count, mean, first, last
import pyspark.sql.functions as F

spark = SparkSession \
    .builder \
    .appName("DfComputed") \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')


# Stream
dfstream = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","df_aggregated") \
    .load()

options = {'sep': ','}
schema = "targa INT, ingresso INT, uscita INT, primo_timestamp TIMESTAMP, ultimo_timestamp TIMESTAMP, count INT"

dfstream = dfstream.selectExpr("CAST(value AS STRING)") \
    .select(from_csv(col("value"), schema, options)
            .alias("data")) \
    .select("data.*")

# Immette i tratti autostradali
tratti = [
    (1, 27, 9, 8.48),
    (2, 9, 26, 17.42),
    (3, 26, 10, 6.0),
    (4, 10, 18, 12.3),
    (5, 18, 23, 14.0),
    (6, 23, 15, 17.6),
    (7, 15, 5, 7.7),
    (8, 5, 8, 10.9),
    (9, 8, 3, 6.9),
    (10, 3, 13, 9.8),
    (11, 22, 1, 10.6),
    (12, 1, 12, 10.9),
    (13, 12, 25, 7.7),
    (14, 25, 20, 17.7),
    (15, 20, 2, 13.8),
    (16, 2, 16, 14.1),
    (17, 16, 4, 14.0),
    (18, 4, 21, 25.7),
]

tratti_schema = StructType(
    [
        StructField("tratto", IntegerType()),
        StructField("ingresso", IntegerType()),
        StructField("uscita", IntegerType()),
        StructField("lunghezza", DoubleType())
    ]
)

df_tratti = spark.createDataFrame(data=tratti, schema=tratti_schema).cache()


# Inizio elaborazione
df_computed = dfstream \
    .filter(col('count') > 1) \
    .join(df_tratti, ['ingresso','uscita'] , 'left').drop('tratto') \
    .withColumn('velocita',  df_tratti.lunghezza / (dfstream['ultimo_timestamp'].cast("long")  - dfstream['primo_timestamp'].cast("long")) * 3600 ) \
    .withColumn('tempo_impiegato (min)',F.round((dfstream['ultimo_timestamp'].cast("long") - dfstream['primo_timestamp'].cast("long")) / 60, 2) ) \
    .filter(col("velocita") > 50).filter(col("velocita") < 300) \
    .groupBy('targa') \
        .agg( 
            first("ingresso").alias("ingresso"),
            first("uscita").alias("uscita"),
            last("lunghezza").alias("lunghezza"),
            F.round(last("velocita"),2).alias("velocita"),
            F.round(last("tempo_impiegato (min)"),2).alias("tempo_impiegato_min"),
            mean("velocita").alias("velocita_media_per_tratto"), 
            F.round(mean("tempo_impiegato (min)"),2).alias("tempo_medio_per_tratto_min"),
        )


def foreach_batch_id(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id)))\
        .write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "dd1") \
        .save()
    pass


# Output in Kafka
print("\n\n\nStarting...\n\n\n")
query = df_computed \
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
