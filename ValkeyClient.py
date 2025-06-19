import hashlib
import json
from valkey import Valkey
from config import VALKEY_HOST, VALKEY_PORT, VALKEY_PASSWORD

from lnhistoryclient.model.types import GossipCache

class ValkeyCache:
    def __init__(self) -> None:
        self.client = Valkey(
            host=VALKEY_HOST,
            port=VALKEY_PORT,
            password=VALKEY_PASSWORD,
            db=0
        )

    @staticmethod
    def hash_raw_hex(raw_hex: str) -> str:
        return hashlib.sha256(raw_hex.encode()).hexdigest()

    def get_metadata_key(self, msg_type: str, msg_hash: str) -> str:
        return f"gossip-test:{msg_type}:{msg_hash}"

    def is_duplicate(self, msg_type: str, msg_hash: str, node_id: str, timestamp: int) -> bool:
        key = self.get_metadata_key(msg_type, msg_hash)
        existing = self.client.get(key)
        if not existing:
            return False  # Completely new hash

        parsed = json.loads(existing)
        timestamps = parsed.get(node_id, [])
        return timestamp in timestamps

    def append_seen_by(self, msg_type: str, msg_hash: str, node_id: str, timestamp: int) -> None:
        key = self.get_metadata_key(msg_type, msg_hash)
        existing = self.client.get(key)

        if existing:
            data = json.loads(existing)
        else:
            data = {}

        if node_id not in data:
            data[node_id] = []

        if timestamp not in data[node_id]:
            data[node_id].append(timestamp)

        self.client.set(key, json.dumps(data))

    def get_seen_from_node_id(self, msg_type: str, msg_hash: str) -> GossipCache:
        key = self.get_metadata_key(msg_type, msg_hash)
        raw = self.client.get(key)
        return json.loads(raw) if raw else {}
