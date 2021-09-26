#! /usr/bin/env python3

from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

# Create a local StreamingContext with two working thread and batch interval of 1 second
if __name__=="__main__":
    # Create a local StreamingContext with two working thread and batch interval of 1 second
    sc = SparkContext("local[2]", "CarCount")
    ssc = StreamingContext(sc, 1)


    lines = KafkaUtils.createDirectStream(ssc, topics=['quickstart-events'],kafkaParams={"metadata.broker.list":"localhost:9092"})

    vincolo = lines.map(lambda x: x[1]).flatmap(lambda line: line.split(";"))
    vincolo.pprint()

    ssc.start()
    ssc.awaitTermination()


df=df.selectExpr("CAST(value AS STRING)")
options = {'sep':';'}
schema = "targa INT, varco INT, corsia DOUBLE, timestamp TIMESTAMP, nazione STRING"
data =  df.select(from_csv(col("value"), schema , options).alias("data")).select("data.*")

num_cars = datastream.groupby("varco").count()
num_cars.writeStream \
    .format("console") \
    .outputMode("complete") \
    .start() \
    .awaitTermination()
