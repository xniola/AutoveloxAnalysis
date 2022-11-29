import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, BooleanType
from pyspark.sql.functions import (
    concat,
    count,
    col,
    from_csv,
    lit,
    count,
    max,
    first,
    struct,
    first,
    last,
    desc
)

spark = SparkSession.builder.appName("EsempioStatico").getOrCreate()

options = {"sep": ","}
schema = "targa INT, varco INT, corsia DOUBLE, timestamp TIMESTAMP, nazione STRING"

df = spark.read.option("header",True) \
    .schema(schema) \
    .csv("../../01.02.2016_cleaned.csv")

df.printSchema()
