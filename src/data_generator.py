import random
import time
from datetime import datetime
import json

def generate_customer_data(customer_id):
    """
    Generate customer data with ID, timestamp, and heart rate.
    
    Args:
        customer_id (int): Unique identifier for the customer
    
    Returns:
        dict: Dictionary containing customer data
    """
    return {
        'customer_id': customer_id,
        'time': datetime.now().isoformat(), # datetime.datetime.now()
        'heart_rate': random.randint(60, 100)  # Normal heart rate range
    }

def get_customer_message():
    """
    Generate a customer message with random customer ID.
    
    Returns:
        bytes: JSON-encoded customer data in UTF-8
    """
    
    if not hasattr(get_customer_message, "counter"):
        get_customer_message.counter = 1
    customer_id = f"cd{get_customer_message.counter:02d}"
    get_customer_message.counter += 1
    data = generate_customer_data(customer_id)
    return json.dumps(data).encode('utf-8')