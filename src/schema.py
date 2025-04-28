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
        StructField('Customer_id', StringType(), False),  # customerID ID (non-nullable integer)
        StructField('Product_id', StringType(), False),  # Product id
        StructField('Product_category', StringType(), True),  # category of products
        StructField('Payment_type', StringType(), True),  # mode of payment
        StructField('Device_Type', StringType(), True),  #device used to access the website 
        StructField('Event_Type', StringType(), True), #date of transaction
        StructField('Event_date', StringType(), True) #date of transaction
    ]
    
   
    
    # Returns a StructType combining all fields for the DataFrame.
    return StructType(basic_fields)
    