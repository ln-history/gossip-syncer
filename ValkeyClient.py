import hashlib
import json

from lnhistoryclient.model.types import GossipIdCacheValue
from valkey import Valkey

from config import VALKEY_HOST, VALKEY_PASSWORD, VALKEY_PORT


class ValkeyCache:
    def __init__(self) -> None:
        self.client = Valkey(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD, db=0)

    @staticmethod
    def hash_raw_bytes(raw_bytes: bytes) -> bytes:
        """Returns a SHA256 hash as bytes"""
        return hashlib.sha256(raw_bytes).digest()
    
    @staticmethod
    def hash_raw_hex(raw_hex: str) -> str:
        """Returns a SHA256 hash as str"""
        return hashlib.sha256(bytes.fromhex(raw_hex)).hexdigest()
    
    
    def get_metadata_key(self, msg_type: int, msg_hash: bytes) -> str:
        """Generates a cache key like gossip:256:abc123"""
        return f"gossip:{msg_type}:{msg_hash}"

    def is_duplicate(self, msg_type: int, msg_hash_hex: str, node_id: str, timestamp: int) -> bool:
        """Checks if a given node_id has already seen the message at this timestamp."""
        key = self.get_metadata_key(msg_type, msg_hash_hex)
        existing = self.client.get(key)

        if not existing:
            return False

        data: GossipIdCacheValue = json.loads(existing)
        return timestamp in data.get(node_id, [])

    def append_seen_by(self, msg_type: int, msg_hash_hex: str, node_id: str, timestamp: int) -> None:
        """Adds the node_id and timestamp to the seen list for the given gossip ID."""
        key = self.get_metadata_key(msg_type, msg_hash_hex)
        existing = self.client.get(key)

        if existing:
            data: GossipIdCacheValue = json.loads(existing)
        else:
            data: GossipIdCacheValue = {}

        timestamps = data.setdefault(node_id, [])
        if timestamp not in timestamps:
            timestamps.append(timestamp)

        self.client.set(key, json.dumps(data))

    def get_seen_from_node_id(self, msg_type: int, msg_hash_hex: str) -> GossipIdCacheValue:
        """Returns all node_id → timestamps entries for a given gossip ID."""
        key = self.get_metadata_key(msg_type, msg_hash_hex)
        raw = self.client.get(key)
        return json.loads(raw) if raw else {}
