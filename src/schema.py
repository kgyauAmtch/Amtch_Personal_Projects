from pyspark.sql.functions import *
from pyspark.sql.types import *

#Explicitly define the structure of the JSON data returned from TMDB before loading it into a PySpark DataFrame

def schema_build():
    """
    Define the schema for the  DataFrame .
    
    Returns:
        StructType: Spark schema for  data
    """
    
    # def generate_fake_data():
    # return [fake.generate_customer_id(), fake.generate_product_id(), fake.product_category(), fake.payment_method(), fake.Device_type(),fake.Event_type(),   get_event_date()]
        # writer.writerow(['Customer_id', 'Product_id', 'Product_category', 'Payment_type', 'Device_Type', 'Event_date'])


    basic_fields = [
        StructField('customer_id', StringType(), False),  # customerID ID (non-nullable integer)
        StructField('product_id', StringType(), False),  # Product id
        StructField('product_category', StringType(), True),  # category of products
        StructField('payment_type', StringType(), True),  # mode of payment
        StructField('device_type', StringType(), True),  #device used to access the website 
        StructField('event_type', StringType(), True), #date of transaction
        StructField('event_date', StringType(), True) #date of transaction
    ]
    
   
    
    # Returns a StructType combining all fields for the DataFrame.
    return StructType(basic_fields)
    