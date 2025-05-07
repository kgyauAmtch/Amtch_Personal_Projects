from kafka import KafkaProducer
import time
from data_generator import get_customer_message
import logging



# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to inform that message delivered to kafka cluster. Logging the topic, partition and offset
def on_send_success(record_metadata):
    print(f'Message delivered to {record_metadata.topic} [{record_metadata.partition}] offset {record_metadata.offset}')

# Function to log error where it fails 
def on_send_error(excp):
    print(f'Message delivery failed: {excp}')


def main():
    # No need for value_serializer, messages are already encoded
    producer = KafkaProducer(bootstrap_servers='kafka:9092')

    topic = 'producer_to_consumer'

    # generate 20,000 rows 
    for _ in range(1, 20000):
        message = get_customer_message()  # Already bytes
        future = producer.send(topic, message)
        future.add_callback(on_send_success)
        future.add_errback(on_send_error)
        logger.info(f'Attempting to send message: {message}')
        time.sleep(5)

    producer.flush()


if __name__ == '__main__':
    main()
