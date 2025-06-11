import os
from dotenv import load_dotenv

load_dotenv()

VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD", None)

ZMQ_HOST = os.getenv("ZMQ_HOST", "localhost")
ZMQ_PORT = os.getenv("ZMQ_PORT", "5675")
ZMQ_TOPIC = os.getenv("ZMQ_TOPIC", "")

KAFKA_TOPIC_TO_PUSH = os.getenv("KAFKA_TOPIC_TO_PUSH", "gossip.all")