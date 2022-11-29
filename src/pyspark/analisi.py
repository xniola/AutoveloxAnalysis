import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import *
import pyspark.sql.functions as F
from pyspark.sql.window import Window
from pyspark.ml.clustering import KMeans
from pyspark.ml.feature import VectorAssembler


# Inizio della sessione spark (static)
spark = SparkSession \
    .builder \
    .appName("Velocita") \
    .getOrCreate()

# Setto il livello dei log da WARN (non mi logga INFO, che sono infinite)
spark.sparkContext.setLogLevel('WARN')

options = {"sep": ","}
schema = "targa INT, varco INT, corsia DOUBLE, timestamp TIMESTAMP, nazione STRING"

#Stream
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe","autovelox") \
    .load()

options = {'sep': ','}
schema = "targa INT, varco INT, corsia DOUBLE, timestamp TIMESTAMP, nazione STRING"

df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_csv(col("value"), schema, options)
            .alias("data")) \
    .select("data.*")

#df.printSchema()

# Immette i tratti autostradali (calcolati manualmente da map.pdf)
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
#df_tratti.show()

# Faccio il left join tra df-df_tratti usando come chiave il varco (sia in ingresso che uscita)
df_left = df.join(df_tratti, (df.varco == df_tratti.ingresso ) | (df.varco == df_tratti.uscita), 'left')
#df_left.show()

# Raggruppa per targa, ingresso e uscita e mostro i relativi timestamp.
df_aggregated = df_left.groupBy(['targa', 'ingresso', 'uscita']) \
    .agg(
        first('timestamp'), 
        last('timestamp'),
        count('timestamp').alias('count') 
    )
#df_aggregated.show()


df_computed = df_aggregated \
    .filter(col('count') > 1) \
    .orderBy('first(timestamp)') \
    .select('targa', 'ingresso', 'uscita', 'first(timestamp)' ,'last(timestamp)')



df_velocita = df_computed.join(df_tratti, ['ingresso','uscita'] , 'left').drop('tratto')
#df_velocita.show()

# Velocita e tempo su tratto 
dd1_tratto = df_velocita \
        .withColumn('velocita',  df_velocita.lunghezza / (df_velocita['last(timestamp)'].cast("long")  - df_velocita['first(timestamp)'].cast("long")) * 3600 ) \
        .withColumn('tempo_impiegato (min)',F.round((df_velocita['last(timestamp)'].cast("long") - df_velocita['first(timestamp)'].cast("long")) / 60, 2) ) \
        .filter(col("velocita") > 50).filter(col("velocita") < 300) \
        .select('targa','ingresso','uscita','lunghezza','tempo_impiegato (min)','velocita')  

# Velocita e tempo medi 
dd2_avg = dd1_tratto.groupBy('targa') \
  .agg( 
      mean("velocita").alias("velocita_media_per_tratto"), 
      F.round(mean("tempo_impiegato (min)"),2).alias("tempo_medio_per_tratto(min)")
  )

'''
# K-means
vecAssembler = VectorAssembler(inputCols=["velocita","tempo_impiegato (min)","lunghezza"], outputCol="livello_pericolo")
new_df = vecAssembler.transform(dd1_tratto)
#new_df.show()

kmeans = KMeans(k=3, seed=1,featuresCol="livello_pericolo")  # 3 clusters
model = kmeans.fit(new_df.select(["livello_pericolo"]))

transformed = model.transform(new_df)
#transformed.show()   

dd3_kmeans = transformed.groupBy('prediction').agg(mean('velocita'), mean('tempo_impiegato (min)'))
'''

# Output
def foreach_batch_id_tratto(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id)))\
        .write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "vel_temp_tratto") \
        .save()
    pass

def foreach_batch_id_avg(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id)))\
        .write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "vel_temp_avg") \
        .save()
    pass

def foreach_batch_id_kmeans(df, epoch_id):
    df.withColumn("key", lit(str(epoch_id)))\
        .write \
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("topic", "kmeans") \
        .save()
    pass

#print("\n\n\nStarting...\n\n\n")
query = dd1_tratto \
    .select(concat(
        "targa", lit(","),
        "ingresso", lit(","),
        "uscita", lit(","),
        "lunghezza", lit(","),
        "tempo_impiegato (min)", lit(","),
        "velocita").alias("value")) \
    .writeStream \
    .foreachBatch(foreach_batch_id_tratto) \
    .outputMode("complete") \
    .start()

query2 = dd2_avg \
    .select(concat(
        "targa", lit(","),
        "velocita_media_per_tratto", lit(","),
        "tempo_medio_per_tratto(min)", lit(",")
        ).alias("value")) \
    .writeStream \
    .foreachBatch(foreach_batch_id_avg) \
    .outputMode("complete") \
    .start()

'''
query3 = dd3_kmeans \
    .select(concat(
        "targa", lit(","),
        "timestamp", lit(","),
        "tratto", lit(","),
        "ingresso", lit(","),
        "avvistamenti", lit(","),
        "nazione").alias("value")) \
    .writeStream \
    .foreachBatch(foreach_batch_id_kmeans) \
    .outputMode("complete") \
    .start()
'''

spark.streams.awaitAnyTermination()