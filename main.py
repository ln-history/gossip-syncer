import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from types import FrameType
from typing import List, Optional

import zmq
from kafka import KafkaProducer
from lnhistoryclient.constants import ALL_TYPES, GOSSIP_TYPE_NAMES
from lnhistoryclient.model.types import PlatformEvent, PlatformEventMetadata, PluginEvent, PluginEventMetadata

import config
from ValkeyClient import ValkeyCache


@dataclass
class ZmqSource:
    host: str
    port: int
    topic: str


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
    bootstrap_servers = f"{config.KAFKA_SERVER_IP_ADDRESS}:{config.KAFKA_SERVER_PORT}"

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


def zmq_worker(
    source: ZmqSource, cache: ValkeyCache, logger: logging.Logger, producer: KafkaProducer, stop_event: threading.Event
) -> None:
    """Worker thread that handles messages from a specific ZMQ source."""
    context = zmq.Context()
    socket = context.socket(zmq.SUB)

    try:
        socket.connect(f"tcp://{source.host}:{source.port}")
        socket.setsockopt_string(zmq.SUBSCRIBE, source.topic)
        logger.info(f"Worker connected to ZeroMQ at {source.host}:{source.port} for topic '{source.topic}'")

        # Configure socket to poll with timeout to check for stop event
        poller = zmq.Poller()
        poller.register(socket, zmq.POLLIN)

        while not stop_event.is_set():
            socks = dict(poller.poll(timeout=1000))  # 1 second timeout

            if socket in socks and socks[socket] == zmq.POLLIN:
                topic = socket.recv_string()
                message = socket.recv_json()
                logger.debug(f"Received message from {source.host}:{source.port} with topic '{topic}'")
                forward_message_if_relevant(message, cache, logger, producer, config.KAFKA_TOPIC_TO_PUSH)

    except Exception as e:
        logger.exception(f"Error in ZMQ worker for {source.host}:{source.port}: {e}")

    finally:
        socket.close()
        logger.info(f"ZMQ worker for {source.host}:{source.port} shut down")


def parse_zmq_sources() -> List[ZmqSource]:
    """Parse ZMQ sources from the config."""
    import config

    sources = []
    if config.ZMQ_SOURCES:
        for src in config.ZMQ_SOURCES:
            sources.append(ZmqSource(**src))
    else:
        sources.append(ZmqSource(host=config.ZMQ_HOST, port=config.ZMQ_PORT, topic=config.ZMQ_TOPIC))

    return sources


def shutdown(logger: logging.Logger, signum: Optional[int] = None, frame: Optional[FrameType] = None) -> None:
    """Clean up resources"""
    global running, producer, stop_event
    logger.info("Shutting down gossip-post-processor...")
    running = False

    # Signal all worker threads to stop
    stop_event.set()  # type: ignore[union-attr]

    if producer:
        producer.close()
        logger.info("Kafka producer closed.")
    logger.info("Shutdown complete.")
    sys.exit(0)


def main() -> None:
    global producer, stop_event, running

    logger = setup_logging()
    logger.info("Starting gossip-syncer...")

    # Initialize shared resources
    cache = ValkeyCache()
    producer = create_kafka_producer()
    stop_event = threading.Event()

    # Parse ZMQ sources from config
    zmq_sources = parse_zmq_sources()
    logger.info(f"Configured {len(zmq_sources)} ZMQ sources")

    # Start worker threads for each ZMQ source
    worker_threads = []
    for source in zmq_sources:
        worker = threading.Thread(
            target=zmq_worker,
            args=(source, cache, logger, producer, stop_event),
            daemon=True,
            name=f"zmq-worker-{source.host}-{source.port}",
        )
        worker.start()
        worker_threads.append(worker)
        logger.info(f"Started worker thread for {source.host}:{source.port}")

    # Register shutdown handler
    signal.signal(signal.SIGINT, lambda s, f: shutdown(logger, s, f))
    signal.signal(signal.SIGTERM, lambda s, f: shutdown(logger, s, f))

    logger.info(f"Gossip-syncer running with {len(worker_threads)} workers... Press Ctrl+C to exit.")

    # Wait for workers to complete (they won't unless stop_event is set)
    try:
        while running:
            signal.pause()
    except (KeyboardInterrupt, SystemExit):
        shutdown(logger)


# Global variables
running: bool = True
producer: Optional[KafkaProducer] = None
stop_event: Optional[threading.Event] = None

if __name__ == "__main__":
    main()
