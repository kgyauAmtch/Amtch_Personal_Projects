# from pyspark.sql import SparkSession
# from pyspark.sql.functions import to_json, struct
# from kafka import KafkaProducer
# from kafka.errors import KafkaError
# import logging as log
import os 
# import schema as sc

from kafka import KafkaProducer
import csv
import time


producer = KafkaProducer(bootstrap_servers='kafka:9092')  # Use 'kafka:9092' as it is in docker

data_dir = '/opt/spark/data/'
topic = 'producer_to_consumer'

def send_csv_to_kafka(file_path):
    with open(file_path, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            message = str(row).encode('utf-8')
            producer.send(topic, value=message)
            print(f"Sent: {message}")
            time.sleep(0.5)  # Simulate real-time

sent_files = set()

while True:
    for filename in os.listdir(data_dir):
        full_path = os.path.join(data_dir, filename)
        if filename.endswith('.csv') and full_path not in sent_files:
            send_csv_to_kafka(full_path)
            sent_files.add(full_path)
    time.sleep(1)
































# spark=SparkSession.builder\
#        .appName('kafka_producer') \
#        .getOrCreate()
            
# spark.sparkContext.setLogLevel("WARN")

# ''' 
# Defined the struct type of columns in dataframe in the function schema_build
# Read the each csv file from the data folder as it generates 
# Display the data in each file in the console and truncate them 
# '''

# # df = spark.read.option("header", "true").option("inferSchema", "true").csv("path/to/file.csv")
# csvDF = spark.readStream.option("sep", ",").schema(sc.schema_build()).csv("/opt/spark/data/")
# # csvDF = spark.readStream.option("sep", ",").schema(sc.schema_build()).csv("/Users/gyauk/github/labs/data")

    
# # Convert to Kafka JSON format
# json_df = csvDF.select(to_json(struct([csvDF[col] for col in csvDF.columns])).alias("value"))

# # Write to Kafka in streaming mode
# query= json_df.writeStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("topic", "producer_to_consumer") \
#     .option("checkpointLocation", "/tmp/kafka_checkpoint") \
#     .start() 



# query.awaitTermination()

# print("Test message sent to Kafka topic.")

    


