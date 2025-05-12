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


# function to writedata to postgresd database
def write_to_postgres(streamed_df, batch_id):
    try:
        print(f"Batch {batch_id} started writing to PostgreSQL...")
        streamed_df.write \
            .format("jdbc") \
            .option("url", "jdbc:postgresql://db:5432/realtimedata") \
            .option("dbtable", "synthetic_data") \
            .option("user", "sparkproj") \
            .option("password", "pass_word") \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()
        print(f"Batch {batch_id} successfully written to PostgreSQL!")
    except Exception as e:
        print(f"Error during batching {batch_id}: {e}")
        
        
    
