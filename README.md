[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Checked with mypy](https://img.shields.io/badge/type%20checked-mypy-blue)](http://mypy-lang.org/)
![Uses: dataclasses](https://img.shields.io/badge/uses-dataclasses-brightgreen)
![Uses: typing](https://img.shields.io/badge/uses-typing-blue)

[![Commitizen friendly](https://img.shields.io/badge/commitizen-friendly-brightgreen.svg)](http://commitizen.github.io/cz-cli/)

# 🔄 gossip-syncer

## 🧠 Summary

`gossip-syncer` is a microservice that listens to a [ZeroMQ](https://zmq.org) stream of gossip messages from the [Bitcoin Lightning Network](https://lightning.network/).
For [Core Lightning](https://corelightning.org/) nodes the [gossip-publisher-zmq plugin](https://github.com/ln-history/gossip-publisher-zmq) can be used. 

For each message:

- It computes a unique `gossip_id` using a [SHA256](https://en.wikipedia.org/wiki/SHA-2) hash of the raw binary
- It checks a [Valkey](https://valkey.io/) (Redis-compatible) cache to determine if this message has been seen before
- Metadata is recorded for each message, regardless of whether it’s new or known
- Only new messages are forwarded to a specified [Kafka](https://kafka.apache.org/) topic

---

## ⚙️ How It Works

### 🔐 Deduplication via Hashing

- Gossip messages are handled in **raw binary** form.
- A SHA256 hash is computed over the full binary, producing a unique `gossip_id`.
- The **first 2 bytes** of the message specify its type, as specified in the [BOLT #7](https://github.com/lightning/bolts/blob/master/07-routing-gossip.md) specification.

### 🧊 Cache Structure (Valkey)

Each message type (`channel_announcement`, `node_announcement`, `channel_update`) maps to a **Redis Hash**.

Each hash maps a `gossip_id` to a list of sender/timestamp entries:

```
Key: gossip:<type> (e.g., gossip:256)
Field: <gossip_id>
Value: {
    "seen_by": {
        "<sender_node_id_1>": ["<timestamp_1>", "<timestamp_2>"],
        "<sender_node_id_2>": ["<timestamp_3>"]
    }
}
```


### ✅ Behavior Overview

#### 🆕 New Gossip Message

1. Compute `gossip_id` from the SHA256 hash of the raw binary.
2. Check if the `gossip_id` exists in the hash for that message type.
3. If not:
   - Add a new entry under the message type's hash.
   - Initialize metadata structure:
     ```json
     {
       "seen_by": {
         "<sender_node_id>": ["<timestamp>"]
       }
     }
     ```
   - Publish the raw message to Kafka.

#### ♻️ Known Gossip Message

If `gossip_id` **already exists**:

- Append the new `<timestamp>` to the list for that `sender_node_id`.
- Do **not** re-publish to Kafka.

#### ❌ Unsupported Type

If the message type is **not in** `[256, 257, 258, 4103]`:

- Log an error.
- Do **not** publish or store.

---

## 📦 Data Format

### 🔍 Message Structure

Each message consumed from ZeroMQ has this JSON shape:

```json
{
  "metadata": {
    "type": 256,                     // Message type from BOLT #7
    "timestamp": "2025-06-11T12:00:00Z",  // Timestamp when the gossip message was seen
    "is_dying": false,              // Status information about the gossip-publisher-zmq plugin (can be ignored)
    "sender_node_id": "029a...",     // Node ID that collected and relayed the message
    "length": 136                    // Byte length excluding 2-byte type prefix
  },
  "raw_hex": "0100abcdef..."         // Full hex-encoded message payload
}
```

---

## 🛠️ Development

### ⚙️ Environment Setup
1. Copy the [.example.env](.example.env) file and rename it to `.env`:

```bash
cp .example.env .env
```

2. Open `.env` and fill in the credentials and configuration values.
💡 Make sure your .env file is never committed to version control—it's ignored via .gitignore.