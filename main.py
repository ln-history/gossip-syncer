import os
import json
import logging
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv(".env")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_kafka_consumer(topic: str) -> KafkaConsumer:
    """Create and return a configured Kafka consumer."""
    bootstrap_servers = f"{os.getenv('SERVER_IP_ADDRESS')}:{os.getenv('SERVER_PORT')}"

    return KafkaConsumer(
        topic,
        bootstrap_servers=[bootstrap_servers],
        client_id="test-python-consumer",
        security_protocol='SASL_SSL',
        ssl_cafile='./kafka.truststore.pem',
        ssl_certfile='./kafka.keystore.pem',
        ssl_keyfile='./kafka.keystore.pem',
        ssl_password=os.getenv("SSL_PASSWORD"),
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
        sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
        ssl_check_hostname=False,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='python-consumer-group',
        value_deserializer=lambda v: json.loads(v.decode('utf-8'))
    )


def consume_messages(consumer: KafkaConsumer):
    logger.info("Listening for messages...")
    for message in consumer:
        logger.info(f"Received message: {message.value}")


def main():
    topic = "test"
    consumer = create_kafka_consumer(topic)
    consume_messages(consumer)


if __name__ == "__main__":
    main()
