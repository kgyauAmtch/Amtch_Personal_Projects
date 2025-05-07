import json
import time
import os
import psycopg2
from kafka import KafkaConsumer
import logging


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_BROKER = 'kafka:9092'
KAFKA_TOPIC = 'producer_to_consumer'
GROUP_ID = 'customer-activity-group'


# PostgreSQL configuration
POSTGRES_HOST = os.environ.get('POSTGRES_HOST', 'db')
POSTGRES_DB = os.environ.get('POSTGRES_DB', 'heart_rate_db')
POSTGRES_USER = os.environ.get('POSTGRES_USER', 'proj5kafka')
POSTGRES_PASSWORD = os.environ.get('POSTGRES_PASSWORD', 'pass_word')

def create_kafka_consumer():
    """Create and return a Kafka consumer instance."""
    retries = 10
    while retries > 0:
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=KAFKA_BROKER,
                group_id=GROUP_ID,
                auto_offset_reset='latest', # read only latest messages
                value_deserializer=lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("Connected to Kafka broker")
            return consumer
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            retries -= 1
            time.sleep(5)
    
    raise Exception("Could not connect to Kafka after multiple attempts")

def connect_to_postgres():
    """Create and return a PostgreSQL connection."""
    retries = 10
    while retries > 0:
        try:
            conn = psycopg2.connect(
                host=POSTGRES_HOST,
                database=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD,
                sslmode="disable"
            )
            logger.info("Connected to PostgreSQL database")
            return conn
        except Exception as e:
            logger.error(f"Failed to connect to PostgreSQL: {e}")
            retries -= 1
            time.sleep(5)
    
    raise Exception("Could not connect to PostgreSQL after multiple attempts")

def main():
    """Main function to consume messages from Kafka and store in PostgreSQL."""
    logger.info("Starting Kafka Consumer")
    
    # Create Kafka consumer
    consumer = create_kafka_consumer()
    
    # Connect to PostgreSQL
    conn = connect_to_postgres()
    cursor = conn.cursor()
    
    try:
        # Process messages
        for message in consumer:
            try:
                data = message.value
                logger.debug(f"Received message: {data}")
                
                # Insert data into PostgreSQL
                cursor.execute(
                    "INSERT INTO heartrate_data (customer_id, heart_rate, time) VALUES (%s, %s, %s)",
                    (data.get('customer_id'), data.get('heart_rate'), data.get('time'))
                )
                conn.commit()
                logger.info(f"Inserted record: {data}")
            
            except Exception as e:
                logger.error(f"Error processing message: {e}")
                conn.rollback()
    
    except KeyboardInterrupt:
        logger.info("Consumer interrupted")
    
    finally:
        # Close connections
        cursor.close()
        conn.close()
        consumer.close()
        logger.info("Consumer closed")

if __name__ == "__main__":
    main()