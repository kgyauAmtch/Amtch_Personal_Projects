from faker import Faker
from datetime import datetime
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

class ProductIDGenerator(BaseProvider):
    
    def __init__(self, generator):
        super().__init__(generator)
        self.counter = 0  

    def generate_product_id(self):
        # Increase the count by 1 and return the ID
        self.counter += 1
        return f"Prod_{1000 + self.counter}"


class ProductCategoryProvider(BaseProvider):
    def product_category(self):
        return random.choice(['Electronics', 'Clothes', 'Accessories', 'Furniture', 'Beauty', 'Hardware', 'Books'])

class PaymentProvider(BaseProvider):
    def payment_method(self):
        return random.choice(['Momo', 'Paypal', 'Visa Card'])
    
class EventProvider(BaseProvider):
    def Event_type(self):
        return random.choice(['Purchase', 'Addtocart'])
    
    
class DeviceProvider(BaseProvider):
    def Device_type(self):
        return random.choice(['Desktop', 'Mobile'])


fake = Faker()

fake.add_provider(CustomerIDGenerator)
fake.add_provider(ProductIDGenerator)
fake.add_provider(ProductCategoryProvider)
fake.add_provider(PaymentProvider)
fake.add_provider(DeviceProvider)
fake.add_provider(EventProvider)



def get_event_date():
    return datetime.strftime(fake.date_time_this_decade(), "%d-%m-%Y")

def product_category():
    return random.randrange(50, 1500000)

def generate_fake_data():
    return [fake.generate_customer_id(), fake.generate_product_id(), fake.product_category(), fake.payment_method(), fake.Device_type(),fake.Event_type(),   get_event_date()]


for x in  range(1,50):
    # with open(f'/Users/gyauk/github/labs/data/ecommerce_data_{x:01d}.csv', 'w', newline='') as csvfile:
    with open(f'/opt/spark/data/ecommerce_data_{x:01d}.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        # writer.writerow(['Customer_id', 'Product_id', 'Product_category', 'Payment_type', 'Device_Type', 'Event_type', 'Event_date'])
        for n in range(1, 1000):
            writer.writerow(generate_fake_data())
    time.sleep(2)

            
        