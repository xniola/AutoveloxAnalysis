
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
    .option("subscribe", "ultimi-avvistamenti") \
    .load()

# Estraggo i dati del csv
dati = dfstream.select(col("value").cast("string"))

# Splitto i dati in colonne (inferisco un dataframe)
df = dati \
      .selectExpr("split(value,',')[0] as targa" ,
                  "split(value,',')[1] as timestamp", \
                  "split(value,',')[2] as tratto", \
                  "split(value,',')[3] as ingresso", \
                  "split(value,',')[4] as avvistamenti", \
                  "split(value,',')[5] as nazione"
                 )

# Query del dataframe appena inferito
query = df     \
  .writeStream  \
  .format("console") \
  .outputMode("update") \
  .start()
query.awaitTermination()