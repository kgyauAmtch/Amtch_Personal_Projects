from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import * 
import helper_functions as hf
import schema as sc


# Initialize Spark
spark = SparkSession.builder \
         .appName("streaming_data") \
         .config("spark.jars", "/opt/spark/work-dir/postgresql-42.7.5.jar") \
         .getOrCreate()
         
spark.sparkContext.setLogLevel("WARN")

''' 
Defined the struct type of columns in dataframe in the function schema_build
Read the each csv file from the data folder as it generates 
Display the data in each file in the console and truncate them 
'''

# df = spark.read.option("header", "true").option("inferSchema", "true").csv("path/to/file.csv")
csvDF = spark.readStream.option("sep", ",").schema(sc.schema_build()).csv("/opt/spark/data/")
csvDF.writeStream.format("console").option("truncate", "false").start()

#conver the columns type to correct datatype
cleaned_df= hf.convert_column_types(csvDF)


query = cleaned_df.writeStream \
    .foreachBatch(hf.write_to_postgres) \
    .outputMode("append") \
    .start()

query.awaitTermination()