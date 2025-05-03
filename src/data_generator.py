from random import randint 
import pandas as pd 
from faker import Faker
from datetime import datetime
import datetime
from faker.providers import BaseProvider
import random
import csv
import time



fake = Faker()
class CustomerIDGenerator(BaseProvider):
    def __init__(self, generator):
        super().__init__(generator)
        self.counter = 0 
    
    def generate_customer_id(self):
        self.counter += 1
        return f"CUST-{1000 + self.counter}"
    
    
class AverageSpeedProvider(BaseProvider):
    def __init__(self, generator):
        super().__init__(generator)
        self.counter = 0  

    def generate_average_speed(self):
        # Increment the counter and return the speed
        speed =round(random.uniform(1.0,12.0),2)
        return f'{speed}m/sec'

class HeartrateProvider(BaseProvider):
    def __init__(self, generator):
        super().__init__(generator)
        self.counter = 0  

    def generate_heart_rate(self):
        # Increment the counter and return the heart rate 
        heart_rate =round(random.uniform(100,210),2)
        return f'{heart_rate}bpm'
       

class ActivityCategoryProvider(BaseProvider):
    def Activity_category(self):
        return random.choice(['Walking', 'Running', 'Hiking', 'Swimming'])


fake = Faker()
fake.add_provider(CustomerIDGenerator)
fake.add_provider(AverageSpeedProvider)
fake.add_provider(HeartrateProvider)
fake.add_provider(ActivityCategoryProvider)



def get_timestamp():
    return datetime.datetime.now()



def generate_fake_data():
    return [fake.generate_customer_id(), fake.generate_heart_rate(), fake.generate_average_speed(),  fake.Activity_category(), get_timestamp()]

# csvfiles=['ecommerce_data1.csv','ecommerce_data2.csv','ecommerce_data3.csv','ecommerce_data4.csv','ecommerce_data5.csv']


for x in  range(1,30):
    with open(f'/Users/gyauk/github/labs/data/heart_beat_{x:01d}.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # writer.writerow(['Customer_id', 'Product_id', 'Product_category', 'Payment_type', 'Device_Type', 'Event_type', 'Event_date'])
        for n in range(1, 1000):
            writer.writerow(generate_fake_data())
    time.sleep(2)

            
        