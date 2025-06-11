# gossip-syncer

## Summary
The micro-service subscribes to a [zero-message-queue](https://zmq.org) to which gossip messages of the [Bitcoin Lightning Network](https://lightning.network/) get sent to.
It calculates an `id` for every gossip message and checks in a [Valkey](https://valkey.io/) cache if this exact gossip message has been read previously. 
when performing the check, metadata of the gossip message gets stored as well.
If it is new it gets published to a [Kafka](https://kafka.apache.org/) instance.

## How it works
The gossip messages get processed in their raw binary form.
This raw binary gets hashed using the [SHA256](https://en.wikipedia.org/wiki/SHA-2) hash function.
The result is the `gossip_id` - unique for every gossip message. 
The first two bits of every gossip message define the message type as specified in [BOLT #7](https://github.com/lightning/bolts/blob/master/07-routing-gossip.md). 
For every gossip message type the cache has a "hashtable" in which the micro service looks.

The hashtable has the following structure:
```
{gossip_id}:seen_by:{sender_node_id}[]<list of timestsamps>
```

There are several cases to handle:

- New gossip message:
Add a new entry in the corresponding hashtable with the `gossip_id`. Add the whole schema {seen_by: ["{sender_node_id}": ["<timestamp of gossip message from zmq>"]]}
Publish the gossip message via the KafkaProducer

- Known gossip message:
If a gossip message get consumed which `gossip_id` is already in the cache, just append the metadata from the zmq-message (sender_node_id and timestamp) accordingly.
Don't publish the message.

- Type of gossip message is not in [256,257,258,4103] -> log an error, don't publish the message


## Data

How does a consumed gossip message look like?

The overall structure of the gossip looks like this:
```json
{
    "metadata": 
        {
            "type": <type of message as specified in BOLT #7>,
            "flags": "flags",
            "timestamp": "timestamp of the collection of the message",
            "is_push": "bool(flags & FLAG_PUSH)",
            "is_dying": "bool(flags & FLAG_DYING)",
            "sender_node_id": <node-id of sending lightning node>,
            "length": "length of message exluding type field (meaning total_length - 2)"
        },
    "raw_hex": "hex of message (Should get parsed to binary)",   
}
```

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
    "timestamp": "2025-06-11T12:00:00Z",  // Timestamp the gossip message was seen
    "is_dying": false,              // Status information about the gossip-publisher-zmq plugin 
    "sender_node_id": "029a...",     // Node ID that relayed the message
    "length": 136                    // Length excluding 2-byte type prefix
  },
  "raw_hex": "0100abcdef..."         // Full hex-encoded message payload
}
