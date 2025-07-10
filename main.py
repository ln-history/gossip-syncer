import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from types import FrameType
from typing import Any, Dict, List, Optional

import zmq
from kafka import KafkaProducer
from lnhistoryclient.constants import ALL_TYPES, GOSSIP_TYPE_NAMES
from lnhistoryclient.model.platform_internal import PlatformEvent
from lnhistoryclient.model.types import PluginEvent, PluginEventMetadata
from lnhistoryclient.parser.parser import parse_platform_event

from config import (
    KAFKA_SASL_PLAIN_PASSWORD,
    KAFKA_SASL_PLAIN_USERNAME,
    KAFKA_SERVER_IP_ADDRESS,
    KAFKA_SERVER_PORT,
    KAFKA_SSL_PASSWORD,
    KAFKA_TOPIC_TO_PUSH,
)
from ValkeyClient import ValkeyCache


@dataclass
class ZmqSource:
    host: str
    port: int
    topic: str


def setup_logging(log_dir: str = "logs", log_file: str = "gossip_syncer.log") -> logging.Logger:
    os.makedirs(log_dir, exist_ok=True)
    logger = logging.getLogger("gossip-syncer")
    logger.setLevel(logging.INFO)

    log_path = os.path.join(log_dir, log_file)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=100)
    file_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s -  %(threadName)s - %(message)s"))

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(threadName)s - %(message)s"))

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
        ssl_password=KAFKA_SSL_PASSWORD,
        sasl_mechanism="SCRAM-SHA-512",
        sasl_plain_username=KAFKA_SASL_PLAIN_USERNAME,
        sasl_plain_password=KAFKA_SASL_PLAIN_PASSWORD,
        ssl_check_hostname=False,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def construct_platform_event(plugin_event: Dict[str, Any], cache: ValkeyCache, logger: logging.Logger) -> PlatformEvent:
    """
    Converts a PluginEvent (from gossip-publisher-zmq) to a validated PlatformEvent using the parse_platform_event() function provided by the lnhistoryclient library.

    This performs transformation from:
      - plugin_event["raw_hex"] → raw_gossip_hex
      - calculates gossip_id = SHA256(bytes.fromhex(raw_gossip_hex))
      - creates metadata including id (as hex string)
      - passes result to `parse_platform_event()` for structure enforcement

    Args:
        plugin_event (Dict[str, Any]): PluginEvent from Core Lightning plugin
        logger (logging.Logger): Logger for diagnostics

    Returns:
        PlatformEvent: Validated event for internal processing

    Raises:
        ValueError: If any field is missing or malformed
    """

    try:
        platform_event = parse_platform_event(plugin_event)
        logger.debug(f"Parsed PlatformEvent: {platform_event}")
        return platform_event

    except ValueError as ve:
        logger.error(f"PluginEvent conversion failed: {ve}")
        raise

    except Exception as e:
        logger.exception(f"Unexpected error during PluginEvent → PlatformEvent conversion: {e}")
        raise


def should_forward_message(plugin_event: PluginEvent, cache: ValkeyCache, logger: logging.Logger) -> bool:
    metadata: PluginEventMetadata = plugin_event.get("metadata", {})
    raw_hex = plugin_event.get("raw_hex")

    required_fields = ("timestamp", "sender_node_id", "type")
    if not all(k in metadata for k in required_fields) or not raw_hex:
        logger.warning("PluginEvent missing required metadata or raw_hex")
        return False

    timestamp = metadata["timestamp"]
    node_id = metadata["sender_node_id"]
    msg_type = metadata["type"]
    gossip_name = GOSSIP_TYPE_NAMES.get(msg_type, f"unknown({msg_type})")

    gossip_id = cache.hash_raw_hex(raw_hex)
    seen_by = cache.get_seen_from_node_id(msg_type, gossip_id)

    if not seen_by:
        logger.info(
            f"New gossip {gossip_name} message with gossip_id {gossip_id} seen for the first time by node {node_id}."
        )
    elif node_id not in seen_by:
        logger.info(f"New node {node_id} for gossip {gossip_name} message with gossip_id {gossip_id} was collected.")
    elif timestamp not in seen_by[node_id]:
        logger.info(
            f"New timestamp {timestamp} from node {node_id} found for gossip {gossip_name} with gossip_id {gossip_id}."
        )
    else:
        logger.debug(
            f"Duplicate gossip {gossip_name} with gossip_id {gossip_id} from {node_id} at {timestamp} found, skipping further handling!"
        )
        return False

    cache.append_seen_by(msg_type, gossip_id, node_id, timestamp)
    return True


def forward_message_if_relevant(
    plugin_event: PluginEvent, cache: ValkeyCache, logger: logging.Logger, producer: KafkaProducer, topic: str
) -> None:
    metadata = plugin_event.get("metadata") or {}
    msg_type = metadata.get("type")
    msg_name = GOSSIP_TYPE_NAMES.get(msg_type, f"unknown({msg_type})")

    if msg_type not in ALL_TYPES:
        logger.error(f"Unknown gossip message type received: {msg_type} when handling PluginEvent {plugin_event}")
        return

    if should_forward_message(plugin_event, cache, logger):
        try:
            platform_event: PlatformEvent = construct_platform_event(plugin_event, cache, logger)
            producer.send(topic, value=platform_event.to_dict())
            logger.info(f"Forwarded {msg_name} message to Kafka topic '{topic}'")
        except Exception as e:
            logger.error(
                f"Failed to construct or send {msg_name} message when handling PluginEvent {plugin_event}: {e}"
            )
    else:
        logger.debug(f"PluginEvent {plugin_event} skipped.")


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
                _ = socket.recv_string()
                message = socket.recv_json()
                forward_message_if_relevant(message, cache, logger, producer, KAFKA_TOPIC_TO_PUSH)

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
