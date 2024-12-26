from kafka import KafkaProducer
import json
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def send_to_kafka(topic, data):
    try:
        producer = KafkaProducer(bootstrap_servers='localhost:9092',
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        producer.send(topic, data)
        producer.flush()
        logger.info(f"Sent message to topic {topic}: {data}")
    except Exception as e:
        logger.error(f"Failed to send message: {str(e)}")

if __name__ == "__main__":
    topic = 'market_data'
    data = {'key': 'testKey', 'value': 'testValue'}  # Replace with your actual data
    send_to_kafka(topic, data)