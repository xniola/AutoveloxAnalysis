#from pyspark.sql import SparkSession
#from pyspark.sql.functions import explode
#from pyspark.sql.functions import split
#from pyspark import spark

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col

import pandas as pd




sc = SparkContext('local')
spark = SparkSession(sc)
#spark.sparkContext.setLogLevel("ERROR")

'''Mappatura dei varchi autostradali con i rispettivi km. Utili per calcolare la distanza chilometrica fra 2 varchi collegati'''
#tratto di strada orizzontale da sinistra verso destra (relativo alla mappa)
or_sx_dx = pd.DataFrame({'varco': [27,9,26,10,18,23,15,5,8,3,13], 'km': [22.6, 31.08,48.5,54.5,66.8,80.8,98.4,106.1,117.8,124.7,134.5]})

#tratto di strada orizzontale da destra verso sinistra (relativo alla mappa)
or_dx_sx = pd.DataFrame({'varco': [22,1,12,25,20,2,16,4,21], 'km': [135.3,120,113.8,106.1,88.4,74.6,60.5,46.5,20.8]})


# Mi iscrivo al topic di kafka
csvDF = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "autovelox") \
    .load()  

# Estraggo i dati del csv
dati = csvDF.select(col("value").cast("string")).alias("csv").select("csv.*")

# Splitto i dati in colonne (inferisco un dataframe)
df = dati \
      .selectExpr("split(value,',')[0] as targa" \
                 ,"split(value,',')[1] as varco" \
                 ,"split(value,',')[2] as corsia" \
                 ,"split(value,',')[3] as timestamp" \
                 ,"split(value,',')[4] as nazione"
                 )

def foreach_batch_function(df, epoch_id):
    df.select("targa").show()

# Query del dataframe appena inferito
query = df     \
  .writeStream  \
  .format("console") \
  .outputMode("append") \
  .foreachBatch(foreach_batch_function)\
  .start()
query.awaitTermination()