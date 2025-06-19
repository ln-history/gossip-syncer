import json
import os

from dotenv import load_dotenv

load_dotenv(".env")

KAFKA_SERVER_IP_ADDRESS = os.getenv("KAFKA_SERVER_IP_ADDRESS", "localhost")
KAFKA_SERVER_PORT = os.getenv("KAFKA_SERVER_PORT", 9092)

VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD", None)

KAFKA_TOPIC_TO_PUSH = os.getenv("KAFKA_TOPIC_TO_PUSH", "gossip.all")

# Optional multiple ZMQ sources (must be a JSON string in .env)
ZMQ_SOURCES = None
ZMQ_SOURCES_RAW = os.getenv("ZMQ_SOURCES")

if ZMQ_SOURCES_RAW:
    try:
        ZMQ_SOURCES = json.loads(ZMQ_SOURCES_RAW)
        if not isinstance(ZMQ_SOURCES, list):
            raise ValueError("ZMQ_SOURCES must be a list of dicts.")
    except Exception as e:
        raise ValueError(f"Invalid ZMQ_SOURCES format: {e}") from e

# Single source fallback
ZMQ_HOST = os.getenv("ZMQ_HOST", "127.0.0.1")
ZMQ_PORT = int(os.getenv("ZMQ_PORT", 5675))
ZMQ_TOPIC = os.getenv("ZMQ_TOPIC", "")
