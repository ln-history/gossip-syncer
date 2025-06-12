import zmq
import json
import os
import logging
import sys
import signal

from logging.handlers import RotatingFileHandler
from kafka import KafkaProducer
from valkey_client import ValkeyCache
from config import ZMQ_HOST, ZMQ_PORT, ZMQ_TOPIC, KAFKA_TOPIC_TO_PUSH, KAFKA_SERVER_IP_ADDRESS, KAFKA_SERVER_PORT
from gossip_types import GOSSIP_TYPE_NAMES, CORE_LIGHTNING_TYPES, PROCESSABLE_TYPES, MSG_TYPE_CHANNEL_DYING


def setup_logging(log_dir="logs", log_file="gossip_unifier.log") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("gossip_unifier")
    logger.setLevel(logging.INFO)

    log_path = os.path.join(log_dir, log_file)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=5)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))

    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    return logger

def create_kafka_producer() -> KafkaProducer:
    bootstrap_servers = f"{KAFKA_SERVER_IP_ADDRESS}:{KAFKA_SERVER_PORT}"
    
    return KafkaProducer(
        bootstrap_servers=[bootstrap_servers],
        client_id="gossip-syncer",
        security_protocol='SASL_SSL',
        ssl_cafile='./certs/kafka.truststore.pem',
        ssl_certfile='./certs/kafka.keystore.pem',
        ssl_keyfile='./certs/kafka.keystore.pem',
        ssl_password=os.getenv("SSL_PASSWORD"),
        sasl_mechanism='SCRAM-SHA-512',
        sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
        sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
        ssl_check_hostname=False,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def construct_message(input_message: dict, cache: ValkeyCache) -> dict:
    """Constructs the outgoing message format with computed id."""
    metadata = input_message.get("metadata", {})
    raw_hex = input_message.get("raw_hex")
    parsed = input_message.get("parsed")

    return {
        "metadata": {
            "id": cache.hash_raw_hex(raw_hex),
            "type": metadata.get("type"),
            "timestamp": metadata.get("timestamp"),
            "length": metadata.get("length"),
            "sender_node_id": metadata.get("sender_node_id")
        },
        "raw_hex": raw_hex,
        "parsed": parsed
    }


def should_forward_message(message: dict, cache: ValkeyCache, logger: logging.Logger) -> bool:
    metadata = message.get("metadata", {})
    raw_hex = message.get("raw_hex")

    if not metadata or not raw_hex:
        logger.warning("Invalid message: missing metadata or raw_hex")
        return False

    timestamp = metadata.get("timestamp")
    node_id = metadata.get("sender_node_id")
    msg_type = metadata.get("type")

    if None in (timestamp, node_id, msg_type):
        logger.warning("Incomplete metadata (timestamp, sender_node_id, type)")
        return False

    msg_name = GOSSIP_TYPE_NAMES.get(msg_type, f"unknown({msg_type})")
    logger.info(f"Handling {msg_name} message.")

    msg_hash = cache.hash_raw_hex(raw_hex)
    seen_by = cache.get_seen_by(msg_type, msg_hash)

    if not seen_by:
        logger.info(f"New gossip message (hash={msg_hash[:8]}...) seen for the first time.")
    elif node_id not in seen_by:
        logger.info(f"New node {node_id} for gossip hash={msg_hash[:8]}...")
    elif timestamp not in seen_by[node_id]:
        logger.info(f"New timestamp {timestamp} for node {node_id} and gossip hash={msg_hash[:8]}...")
    else:
        logger.debug(f"Duplicate message from {node_id} at {timestamp}, skipping.")
        return False

    cache.append_seen_by(msg_type, msg_hash, node_id, timestamp)
    return True


def forward_message_if_relevant(message: dict, cache: ValkeyCache, logger: logging.Logger, producer, topic: str):
    msg_type = message.get("metadata", {}).get("type")
    msg_name = GOSSIP_TYPE_NAMES.get(msg_type, f"unknown({msg_type})")

    if msg_type in PROCESSABLE_TYPES:
        if should_forward_message(message, cache, logger):
            constructed = construct_message(message, cache)
            producer.send(topic, value=constructed)
            logger.info(f"Forwarded {msg_name} message to Kafka topic {topic}")
        else:
            logger.debug("Message skipped.")
    elif msg_type in CORE_LIGHTNING_TYPES:
        logger.info(f"Core Lightning specific gossip message collected (type={msg_type})")
        if msg_type == MSG_TYPE_CHANNEL_DYING:
            logger.info(f"Forwarded {msg_name} message to Kafka topic {topic}")
            producer.send(topic, value=message)
    else:
        logger.warning(f"Unknown gossip message type received: {msg_type}")

def shutdown(logger: logging.Logger, signum=None, frame=None):
    """Clean up resources"""
    global running, consumer, postgres
    logger.info("Shutting down GossipSyncer...")
    running = False
    if consumer:
        consumer.close()
        logger.info("Kafka consumer closed.")
    if postgres:
        postgres.close()
        logger.info("PostgreSQL connection closed.")
    logger.info("Shutdown complete.")
    sys.exit(0)

def main():
    logger = setup_logging()
    logger.info("Starting gossip-syncer...")

    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    socket.connect(f"tcp://{ZMQ_HOST}:{ZMQ_PORT}")
    socket.setsockopt_string(zmq.SUBSCRIBE, ZMQ_TOPIC)
    logger.info(f"Subscribed to ZeroMQ at {ZMQ_HOST}:{ZMQ_PORT} at topic '{ZMQ_TOPIC}'")

    cache = ValkeyCache()
    producer = create_kafka_producer()

    # Register shutdown handler
    signal.signal(signal.SIGINT, shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    logger.info("Starting gossip-syncer... Press Ctrl+C to exit.")

    while True:
        try:
            topic = socket.recv_string()  
            message = socket.recv_json()
            forward_message_if_relevant(message, cache, logger, producer, KAFKA_TOPIC_TO_PUSH)
        except Exception as e:
            logger.exception(f"Unexpected error in main loop: {e}")

if __name__ == "__main__":
    main()