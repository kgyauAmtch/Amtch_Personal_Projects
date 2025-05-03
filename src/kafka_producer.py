from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct
from kafka import KafkaProducer
from kafka.errors import KafkaError
import logging as log
import os 

spark=SparkSession.builder\
       .appname('kafka_producer') \
       .getOrCreate()
       


producer = KafkaProducer(bootstrap_servers=['localhost:9092'])


         
spark.sparkContext.setLogLevel("WARN")

''' 
Defined the struct type of columns in dataframe in the function schema_build
Read the each csv file from the data folder as it generates 
Display the data in each file in the console and truncate them 
'''

# df = spark.read.option("header", "true").option("inferSchema", "true").csv("path/to/file.csv")
# csvDF = spark.readStream.option("sep", ",").schema(sc.schema_build()).csv("/opt/spark/data/")
csvDF = spark.readStream.option("sep", ",").schema(sc.schema_build()).csv("/data")
# csvDF.writeStream.format("console").option("truncate", "false").start()

# Convert to Kafka JSON format
json_df = csvDF.select(to_json(struct([csvDF[col] for col in csvDF.columns])).alias("value"))

# Write to Kafka in streaming mode
json_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "producer_to_consumer") \
    .option("checkpointLocation", "/tmp/kafka_checkpoint") \
    .start() \
    .awaitTermination()


    


