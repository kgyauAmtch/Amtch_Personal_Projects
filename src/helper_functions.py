import pyspark.sql.functions 
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

def convert_column_types(df):
     df = df.withColumn("Customer_ID", col("Customer_ID").cast(StringType())) \
           .withColumn("Product_id", col("Product_id").cast(StringType())) \
           .withColumn("Product_category", col("Product_category").cast(StringType())) \
           .withColumn("Payment_type", col("Payment_type").cast(StringType())) \
           .withColumn("Device_Type", col("Device_Type").cast(StringType())) \
           .withColumn("Event_Type", col("Event_Type").cast(StringType())) \
           .withColumn("Event_date", to_date(col("Event_date"), "yyyy-MM-dd"))
     df.printSchema()
     return df
    

# # Assume `streaming_df` is your transformed streaming DataFrame

# def write_to_postgres(batch_df, batch_id):
#     batch_df.write \
#         .format("jdbc") \
#         .option("url", "jdbc:postgresql://localhost:5432/realtimedata") \
#         .option("dbtable", "synthetic_data") \
#         .option("user", "your_username") \
#         .option("password", "gyaurocks99") \
#         .option("driver", "org.postgresql.Driver") \
#         .mode("append") \
#         .save()

# # Attach this function to your streaming query
# query = cleaned_df.writeStream \
#     .foreachBatch(write_to_postgres) \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()
