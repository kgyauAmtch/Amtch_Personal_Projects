import pyspark.sql.functions 
from pyspark.sql import functions as F
from pyspark.sql.functions import *
from pyspark.sql.types import *

def convert_column_types(df):
     df = df.withColumn("customer_id", col("customer_id").cast(StringType())) \
           .withColumn("product_id", col("product_id").cast(StringType())) \
           .withColumn("product_category", col("product_category").cast(StringType())) \
           .withColumn("payment_type", col("payment_type").cast(StringType())) \
           .withColumn("device_type", col("device_type").cast(StringType())) \
           .withColumn("event_type", col("event_type").cast(StringType())) \
           .withColumn("event_date", to_date(col("event_date"), "yyyy-MM-dd"))
     df.printSchema()
     return df
    

# Assume `streaming_df` is your transformed streaming DataFrame


def write_to_postgres(streamed_df, batch_id):
    try:
        print(f"Batch {batch_id} started writing to PostgreSQL...")
        streamed_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://localhost:5433/realtimedata") \
            .option("dbtable", "synthetic_data") \
            .option("user", "proj4streamdata") \
            .option("password", "pass_word") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} successfully written to PostgreSQL!")
    except Exception as e:
        print(f"Error during batching {batch_id}: {e}")
        
        
        
        
    # print(f"Batch {batch_id} successfully written to PostgreSQL!")
    # except Exception as e:
    # print(f"Error during batch {batch_id}: {e}")

# # Attach this function to your streaming query
# query = cleaned_df.writeStream \
#     .foreachBatch(write_to_postgres) \
#     .outputMode("append") \
#     .start()

# query.awaitTermination()
