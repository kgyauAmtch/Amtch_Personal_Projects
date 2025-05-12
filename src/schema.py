from pyspark.sql.functions import *
from pyspark.sql.types import *

#Explicitly define the structure of the JSON data before loading it into a PySpark DataFrame

def schema_build():
    """
    Define the schema for the  DataFrame .
    
    Returns:
        StructType: Spark schema for  data
    """


    basic_fields = [
        StructField('customer_id', StringType(), False),  # customerid
        StructField('product_id', StringType(), False),  # Product id
        StructField('product_category', StringType(), True),  # category of products
        StructField('payment_type', StringType(), True),  # mode of payment
        StructField('device_type', StringType(), True),  #device used to access the website 
        StructField('event_type', StringType(), True), #type of transaction
        StructField('event_date', StringType(), True) #date of transaction
    ]
    
   
    
    # Returns a StructType combining all fields(rows) for the DataFrame.
    return StructType(basic_fields)
    