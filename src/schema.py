from pyspark.sql.functions import *
from pyspark.sql.types import *

def schema_build():

 basic_fields = [
        StructField('customer_id', StringType(), False),  # customerID ID (non-nullable integer)
        StructField('Heart_rate', StringType(), False),  # current heart raet in bpm
        StructField('Average_speed', StringType(), True),  # average speed of customer in m/s
        StructField('Activity', StringType(), True),  # Actvity of user
        StructField('Time', TimestampType(), True) #date of transaction
 ]
 

 return StructType(basic_fields)
    
    

