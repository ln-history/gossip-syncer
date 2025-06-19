import json
import logging
import os
import signal
import sys
from logging.handlers import RotatingFileHandler
from types import FrameType
from typing import Optional

import zmq
from kafka import KafkaProducer
from lnhistoryclient.constants import ALL_TYPES, GOSSIP_TYPE_NAMES
from lnhistoryclient.model.types import PlatformEvent, PlatformEventMetadata, PluginEvent, PluginEventMetadata

from config import KAFKA_SERVER_IP_ADDRESS, KAFKA_SERVER_PORT, KAFKA_TOPIC_TO_PUSH, ZMQ_HOST, ZMQ_PORT, ZMQ_TOPIC
from ValkeyClient import ValkeyCache


def setup_logging(log_dir: str = "logs", log_file: str = "gossip_syncer.log") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("gossip_syncer")
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
        security_protocol="SASL_SSL",
        ssl_cafile="./certs/kafka.truststore.pem",
        ssl_certfile="./certs/kafka.keystore.pem",
        ssl_keyfile="./certs/kafka.keystore.pem",
        ssl_password=os.getenv("SSL_PASSWORD"),
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=os.getenv("SASL_PLAIN_USERNAME"),
        sasl_plain_password=os.getenv("SASL_PLAIN_PASSWORD"),
        ssl_check_hostname=False,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def construct_lightning_gossip_message(plugin_event: PluginEvent, cache: ValkeyCache) -> PlatformEvent:
    """Constructs the outgoing message format with computed id."""
    plugin_event_metadata = plugin_event.get("metadata", {})

    raw_hex = plugin_event.get("raw_hex")
    metadata: PlatformEventMetadata = {
        "id": cache.hash_raw_hex(raw_hex),
        "type": plugin_event_metadata.get("type"),
        "timestamp": plugin_event_metadata.get("timestamp"),
    }

    return {
        "metadata": metadata,
        "raw_hex": raw_hex,
    }


def should_forward_message(plugin_event: PluginEvent, cache: ValkeyCache, logger: logging.Logger) -> bool:
    metadata: PluginEventMetadata = plugin_event.get("metadata", {})
    raw_hex = plugin_event.get("raw_hex")

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
    seen_by = cache.get_seen_from_node_id(msg_type, msg_hash)

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


def forward_message_if_relevant(
    message: PluginEvent, cache: ValkeyCache, logger: logging.Logger, producer: KafkaProducer, topic: str
) -> None:
    msg_type = message.get("metadata", {}).get("type")
    msg_name = GOSSIP_TYPE_NAMES.get(msg_type, f"unknown({msg_type})")

    if msg_type in ALL_TYPES:
        if should_forward_message(message, cache, logger):
            constructed = construct_lightning_gossip_message(message, cache)
            producer.send(topic, value=constructed)
            logger.info(f"Forwarded {msg_name} message to Kafka topic {topic}")
        else:
            logger.debug("Message skipped.")
    else:
        logger.error(f"Unknown gossip message type received: {msg_type}")


def shutdown(logger: logging.Logger, signum: Optional[int] = None, frame: Optional[FrameType] = None) -> None:
    """Clean up resources"""
    global running, producer
    logger.info("Shutting down gossip-post-processor...")
    running = False
    if producer:
        producer.close()
        logger.info("Kafka consumer closed.")
    logger.info("Shutdown complete.")
    sys.exit(0)


def main() -> None:
    global producer

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
    signal.signal(signal.SIGINT, lambda s, f: shutdown(logger, s, f))
    signal.signal(signal.SIGTERM, lambda s, f: shutdown(logger, s, f))

    logger.info("Starting gossip-syncer... Press Ctrl+C to exit.")

    while True:
        try:
            topic = socket.recv_string()  # Irrelevant
            message = socket.recv_json()
            forward_message_if_relevant(message, cache, logger, producer, KAFKA_TOPIC_TO_PUSH)
        except Exception as e:
            logger.exception(f"Unexpected error in main loop: {e}")


# Global variables
running: bool = True
producer: Optional[KafkaProducer] = None

if __name__ == "__main__":
    main()
