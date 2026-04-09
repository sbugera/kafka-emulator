"""
Kafka Protocol Emulator
=======================
A single-port TCP server that speaks the Kafka binary wire protocol.
Java Kafka clients (producer/consumer) can connect without any code changes —
just point them at host:9092 (or whatever port you configure).

Supported Kafka API keys:
  0  - Produce
  1  - Fetch
  2  - ListOffsets
  3  - Metadata
  9  - OffsetFetch
  10 - FindCoordinator
  11 - JoinGroup
  12 - Heartbeat
  13 - LeaveGroup
  14 - SyncGroup
  18 - ApiVersions

Usage:
    python kafka_emulator.py [--host HOST] [--port PORT]

Defaults: host=localhost, port=9092
"""

import asyncio
import inspect
import struct
import logging
import argparse
import uuid
import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
log = logging.getLogger("kafka-emulator")

# Delay (seconds) added to every Fetch response that contains data.
# Caps the retry-loop rate when a listener keeps failing; also controls
# how quickly a consumer sees newly produced messages.
FETCH_DATA_DELAY_S: float = 5.0

# ---------------------------------------------------------------------------
# In-memory storage
# ---------------------------------------------------------------------------

@dataclass
class Message:
    offset: int
    key: Optional[bytes]
    value: bytes
    timestamp: int
    headers: list


class TopicPartition:
    def __init__(self):
        self.messages: List[Message] = []
        self.next_offset: int = 0

    def append(self, key, value, timestamp, headers) -> int:
        offset = self.next_offset
        self.messages.append(Message(offset, key, value, timestamp, headers))
        self.next_offset += 1
        return offset

    def fetch(self, start_offset: int, max_bytes: int) -> List[Message]:
        result = []
        size = 0
        for msg in self.messages:
            if msg.offset < start_offset:
                continue
            size += len(msg.value or b"") + 100
            result.append(msg)
            if size >= max_bytes:
                break
        return result


class Broker:
    """Central state for topics, groups, and offsets."""

    def __init__(self):
        # topic -> partition_id -> TopicPartition
        self.topics: Dict[str, Dict[int, TopicPartition]] = defaultdict(
            lambda: defaultdict(TopicPartition)
        )
        # group_id -> member_id -> {"topics": [...], "client_id": str}
        self.groups: Dict[str, Dict[str, dict]] = defaultdict(dict)
        # group_id -> generation_id
        self.group_generation: Dict[str, int] = defaultdict(int)
        # group_id -> leader member_id
        self.group_leader: Dict[str, str] = {}
        # (group_id, topic, partition) -> offset
        self.committed_offsets: Dict[Tuple, int] = {}
        # pending join requests: group_id -> [Future]
        self._join_waiters: Dict[str, list] = defaultdict(list)
        self._join_timer: Dict[str, asyncio.TimerHandle] = {}
        self.REBALANCE_TIMEOUT = 0.3  # seconds to wait for more joiners
        self._sync_events: Dict[str, asyncio.Event] = {}

    def ensure_topic(self, topic: str, num_partitions: int = 1):
        for p in range(num_partitions):
            _ = self.topics[topic][p]  # triggers defaultdict creation

    def produce(self, topic: str, partition: int, key, value, headers=None) -> int:
        ts = int(time.time() * 1000)
        offset = self.topics[topic][partition].append(key, value, ts, headers or [])
        log.info(f"  PRODUCE → topic={topic} partition={partition} offset={offset} size={len(value or b'')}B")
        return offset

    def fetch(self, topic: str, partition: int, start_offset: int, max_bytes: int):
        return self.topics[topic][partition].fetch(start_offset, max_bytes)

    def commit_offset(self, group_id: str, topic: str, partition: int, offset: int):
        self.committed_offsets[(group_id, topic, partition)] = offset

    def get_offset(self, group_id: str, topic: str, partition: int) -> int:
        return self.committed_offsets.get((group_id, topic, partition), -1)

    def list_offsets(self, topic: str, partition: int, timestamp: int) -> int:
        tp = self.topics[topic][partition]
        if timestamp == -2:  # earliest
            return 0
        if timestamp == -1:  # latest
            return tp.next_offset
        # find first offset >= timestamp
        for msg in tp.messages:
            if msg.timestamp >= timestamp:
                return msg.offset
        return tp.next_offset


BROKER = Broker()


# ---------------------------------------------------------------------------
# Binary codec helpers
# ---------------------------------------------------------------------------

class Reader:
    def __init__(self, data: bytes):
        self.buf = data
        self.pos = 0

    def read(self, n: int) -> bytes:
        v = self.buf[self.pos:self.pos + n]
        self.pos += n
        return v

    def int8(self) -> int:
        return struct.unpack(">b", self.read(1))[0]

    def int16(self) -> int:
        return struct.unpack(">h", self.read(2))[0]

    def int32(self) -> int:
        return struct.unpack(">i", self.read(4))[0]

    def int64(self) -> int:
        return struct.unpack(">q", self.read(8))[0]

    def uint32(self) -> int:
        return struct.unpack(">I", self.read(4))[0]

    def string(self) -> Optional[str]:
        n = self.int16()
        if n < 0:
            return None
        return self.read(n).decode("utf-8", errors="replace")

    def bytes_(self) -> Optional[bytes]:
        n = self.int32()
        if n < 0:
            return None
        return self.read(n)

    def compact_string(self) -> Optional[str]:
        n = self._varint_u() - 1
        if n < 0:
            return None
        return self.read(n).decode("utf-8", errors="replace")

    def compact_bytes(self) -> Optional[bytes]:
        n = self._varint_u() - 1
        if n < 0:
            return None
        return self.read(n)

    def _varint_u(self) -> int:
        result, shift = 0, 0
        while True:
            b = self.read(1)[0]
            result |= (b & 0x7F) << shift
            shift += 7
            if not (b & 0x80):
                return result

    def varint(self) -> int:
        u = self._varint_u()
        return (u >> 1) ^ -(u & 1)

    def remaining(self) -> int:
        return len(self.buf) - self.pos


class Writer:
    def __init__(self):
        self.buf = bytearray()

    def int8(self, v):
        self.buf += struct.pack(">b", v)
        return self

    def int16(self, v):
        self.buf += struct.pack(">h", v)
        return self

    def int32(self, v):
        self.buf += struct.pack(">i", v)
        return self

    def int64(self, v):
        self.buf += struct.pack(">q", v)
        return self

    def uint32(self, v):
        self.buf += struct.pack(">I", v)
        return self

    def string(self, s: Optional[str]):
        if s is None:
            self.int16(-1)
        else:
            enc = s.encode("utf-8")
            self.int16(len(enc))
            self.buf += enc
        return self

    def bytes_(self, b: Optional[bytes]):
        if b is None:
            self.int32(-1)
        else:
            self.int32(len(b))
            self.buf += b
        return self

    def raw(self, b: bytes):
        self.buf += b
        return self

    def _varint_u(self, v: int):
        while True:
            b = v & 0x7F
            v >>= 7
            if v:
                self.buf.append(b | 0x80)
            else:
                self.buf.append(b)
                break

    def compact_string(self, s: Optional[str]):
        if s is None:
            self._varint_u(0)
        else:
            enc = s.encode("utf-8")
            self._varint_u(len(enc) + 1)
            self.buf += enc
        return self

    def compact_bytes(self, b: Optional[bytes]):
        if b is None:
            self._varint_u(0)
        else:
            self._varint_u(len(b) + 1)
            self.buf += b
        return self

    def tagged_fields(self):
        self._varint_u(0)
        return self

    def build(self) -> bytes:
        return bytes(self.buf)


def frame(correlation_id: int, body: bytes, flexible: bool = False) -> bytes:
    # flexible=True: Response Header v1 — append a 0x00 tagged-fields byte
    # after the correlation_id (required for flexible/compact API versions).
    if flexible:
        resp = struct.pack(">i", correlation_id) + b'\x00' + body
    else:
        resp = struct.pack(">i", correlation_id) + body
    return struct.pack(">i", len(resp)) + resp


# ---------------------------------------------------------------------------
# Record batch encoder/decoder
# ---------------------------------------------------------------------------

def decode_record_batch(data: bytes) -> List[dict]:
    """Decode a Kafka v2 record batch (or fall back to legacy)."""
    records = []
    r = Reader(data)
    try:
        while r.remaining() >= 12:
            base_offset = r.int64()
            batch_len = r.int32()
            if batch_len <= 0 or r.remaining() < batch_len:
                break
            batch_data = r.read(batch_len)
            br = Reader(batch_data)
            partition_leader_epoch = br.int32()
            magic = br.int8()
            if magic == 2:
                records += _decode_batch_v2(base_offset, br)
            else:
                records += _decode_legacy(base_offset, magic, br)
    except Exception as e:
        log.debug(f"Record batch decode error: {e}")
    return records


def _decode_batch_v2(base_offset: int, r: Reader) -> List[dict]:
    records = []
    _crc = r.int32()  # noqa
    _attrs = r.int16()
    _last_offset_delta = r.int32()
    _base_ts = r.int64()
    _max_ts = r.int64()
    _producer_id = r.int64()
    _producer_epoch = r.int16()
    _base_seq = r.int32()
    rec_count = r.int32()
    for _ in range(rec_count):
        try:
            _len = r.varint()
            _attrs = r.int8()
            _ts_delta = r.varint()
            _offset_delta = r.varint()
            key_len = r.varint()
            key = r.read(key_len) if key_len > 0 else None
            val_len = r.varint()
            value = r.read(val_len) if val_len > 0 else b""
            hdr_count = r.varint()
            headers = []
            for _ in range(hdr_count):
                hk_len = r.varint()
                hk = r.read(hk_len).decode() if hk_len > 0 else ""
                hv_len = r.varint()
                hv = r.read(hv_len) if hv_len > 0 else b""
                headers.append((hk, hv))
            records.append({"key": key, "value": value, "headers": headers})
        except Exception:
            break
    return records


def _decode_legacy(base_offset: int, magic: int, r: Reader) -> List[dict]:
    # Best-effort for magic 0/1
    records = []
    try:
        _attrs = r.int8()
        if magic == 1:
            _ts = r.int64()
        key = r.bytes_()
        value = r.bytes_()
        records.append({"key": key, "value": value, "headers": []})
    except Exception:
        pass
    return records


def encode_record_batch(messages: List[Message]) -> bytes:
    """Encode messages as a Kafka v2 record batch."""
    if not messages:
        return b""
    base_offset = messages[0].offset
    base_ts = messages[0].timestamp

    records_buf = bytearray()
    for i, msg in enumerate(messages):
        rec = _encode_record_v2(
            offset_delta=msg.offset - base_offset,
            ts_delta=msg.timestamp - base_ts,
            key=msg.key,
            value=msg.value,
            headers=msg.headers,
        )
        records_buf += rec

    batch_inner = bytearray()
    batch_inner += struct.pack(">i", 0)          # partition leader epoch
    batch_inner += struct.pack(">b", 2)          # magic = 2
    batch_inner += struct.pack(">I", 0)          # crc (filled below)
    batch_inner += struct.pack(">h", 0)          # attributes
    batch_inner += struct.pack(">i", len(messages) - 1)  # last offset delta
    batch_inner += struct.pack(">q", base_ts)    # base timestamp
    batch_inner += struct.pack(">q", messages[-1].timestamp)  # max timestamp
    batch_inner += struct.pack(">q", -1)         # producer id
    batch_inner += struct.pack(">h", -1)         # producer epoch
    batch_inner += struct.pack(">i", -1)         # base sequence
    batch_inner += struct.pack(">i", len(messages))  # record count
    batch_inner += records_buf
    # CRC32c covers everything after the CRC field (from attributes onward)
    struct.pack_into(">I", batch_inner, 5, _crc32c(bytes(batch_inner[9:])))

    result = bytearray()
    result += struct.pack(">q", base_offset)
    result += struct.pack(">i", len(batch_inner))
    result += batch_inner
    return bytes(result)


def _crc32c(data: bytes) -> int:
    """CRC32c (Castagnoli) — required by Kafka v2 record batch format."""
    crc = 0xFFFFFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0x82F63B78 if crc & 1 else crc >> 1
    return crc ^ 0xFFFFFFFF


def _varint_encode(v: int) -> bytes:
    # zigzag encode
    v = (v << 1) ^ (v >> 63)
    out = bytearray()
    while True:
        b = v & 0x7F
        v >>= 7
        if v:
            out.append(b | 0x80)
        else:
            out.append(b)
            break
    return bytes(out)




def _encode_record_v2(offset_delta, ts_delta, key, value, headers) -> bytes:
    body = bytearray()
    body += struct.pack(">b", 0)          # attributes
    body += _varint_encode(ts_delta)
    body += _varint_encode(offset_delta)
    if key:
        body += _varint_encode(len(key))
        body += key
    else:
        body += _varint_encode(-1)
    if value:
        body += _varint_encode(len(value))
        body += value
    else:
        body += _varint_encode(-1)
    body += _varint_encode(len(headers))
    for hk, hv in headers:
        hk_enc = hk.encode()
        body += _varint_encode(len(hk_enc))
        body += hk_enc
        body += _varint_encode(len(hv))
        body += hv

    out = _varint_encode(len(body)) + bytes(body)
    return out


# ---------------------------------------------------------------------------
# Request handlers
# ---------------------------------------------------------------------------

HOST = "127.0.0.1"


def handle_api_versions(r: Reader, api_version: int, correlation_id: int) -> bytes:
    # Caps are set to the highest version each handler's *response* fully
    # implements. Response-field additions that force caps down:
    #   Produce  v8: adds record_errors + error_message per partition
    #   Fetch    v8: adds preferred_read_replica per partition
    #   ListOffsets v5: adds leader_epoch per partition
    #   Metadata v8: adds cluster/topic authorized_operations fields
    #   OffsetFetch v6: adds leader_epoch per partition
    # Flexible-encoding boundary (hard cap unless implemented with flexible encoding):
    #   Produce v9+, Fetch v12+, ListOffsets v6+, Metadata v9+, OffsetFetch v8+,
    #   JoinGroup v6+, Heartbeat/LeaveGroup/SyncGroup v4+.
    #   FindCoordinator v4+ IS implemented with flexible encoding (cap raised to v5).
    apis = [
        (0, 0, 7),    # Produce
        (1, 0, 7),    # Fetch
        (2, 0, 4),    # ListOffsets
        (3, 0, 7),    # Metadata
        (8, 0, 7),    # OffsetCommit (v8+ flexible; cap at v7)
        (9, 0, 5),    # OffsetFetch
        (10, 0, 5),   # FindCoordinator (v4+ flexible with coordinator array)
        (11, 0, 5),   # JoinGroup
        (12, 0, 3),   # Heartbeat
        (13, 0, 3),   # LeaveGroup
        (14, 0, 3),   # SyncGroup
        (18, 0, 3),   # ApiVersions — we now support v3 (flexible encoding)
        (22, 0, 3),   # InitProducerId
    ]

    if api_version >= 3:
        # v3+ uses flexible/compact encoding:
        #   response body: int16 error_code, compact_array api_keys,
        #                  int32 throttle_time_ms, varint tagged_fields
        #   each api_key entry: int16×3 + varint tagged_fields
        #   response header: correlation_id + varint tagged_fields (flexible=True)
        w = Writer()
        w.int16(0)               # error_code
        w._varint_u(len(apis) + 1)  # compact array: N+1 encodes N elements
        for api_key, min_v, max_v in apis:
            w.int16(api_key).int16(min_v).int16(max_v)
            w._varint_u(0)       # per-entry tagged fields
        w.int32(0)               # throttle_time_ms
        w._varint_u(0)           # top-level tagged fields
        # ApiVersions always uses Response Header v0 (no tagged-fields byte
        # after correlation_id), even for v3+. It is the bootstrap request
        # used before version negotiation completes, so the header format is
        # fixed. Only the body uses flexible/compact encoding.
        return frame(correlation_id, w.build())

    w = Writer()
    w.int16(0)  # error_code
    w.int32(len(apis))
    for api_key, min_v, max_v in apis:
        w.int16(api_key).int16(min_v).int16(max_v)
    if api_version >= 1:
        w.int32(0)   # throttle_time_ms (added in v1; absent in v0)
    return frame(correlation_id, w.build())


def handle_metadata(r: Reader, api_version: int, correlation_id: int) -> bytes:
    topic_count = r.int32()
    requested = []
    for _ in range(topic_count):
        t = r.string()
        if t:
            requested.append(t)

    # Auto-create topics on metadata request
    for t in requested:
        BROKER.ensure_topic(t, num_partitions=1)

    # If empty request, return all known topics
    topics = requested if requested else list(BROKER.topics.keys())

    w = Writer()
    w.int32(0)  # throttle_time_ms
    # brokers array
    w.int32(1)
    w.int32(1)           # node_id
    w.string(HOST)
    w.int32(9092)
    w.string(None)       # rack

    if api_version >= 2:
        w.string("cluster-1")
        w.int32(1)       # controller_id

    w.int32(len(topics))
    for topic in topics:
        BROKER.ensure_topic(topic)
        partitions = BROKER.topics[topic]
        w.int16(0)       # error_code
        w.string(topic)
        if api_version >= 1:
            w.int8(0)    # is_internal
        w.int32(len(partitions))
        for pid in sorted(partitions.keys()):
            w.int16(0)   # error_code
            w.int32(pid) # partition_index
            w.int32(1)   # leader_id
            if api_version >= 7:
                w.int32(0)   # leader_epoch
            w.int32(1)   # replicas count
            w.int32(1)   # replica node
            w.int32(1)   # isr count
            w.int32(1)   # isr node
            if api_version >= 5:
                w.int32(0)   # offline_replicas count
    return frame(correlation_id, w.build())


def handle_produce(r: Reader, api_version: int, correlation_id: int) -> bytes:
    if api_version >= 3:
        _transactional_id = r.string()
    _acks = r.int16()
    _timeout_ms = r.int32()
    topic_count = r.int32()

    results = []
    for _ in range(topic_count):
        topic = r.string()
        partition_count = r.int32()
        part_results = []
        for _ in range(partition_count):
            partition = r.int32()
            record_set_size = r.int32()
            record_data = r.read(record_set_size)
            records = decode_record_batch(record_data)
            for rec in records:
                BROKER.produce(topic, partition, rec["key"], rec["value"], rec["headers"])
            log.info(f"PRODUCE topic={topic} partition={partition} count={len(records)}")
            last_offset = BROKER.topics[topic][partition].next_offset - 1
            part_results.append((partition, last_offset))
        results.append((topic, part_results))

    w = Writer()
    w.int32(len(results))
    for topic, part_results in results:
        w.string(topic)
        w.int32(len(part_results))
        for partition, last_offset in part_results:
            w.int32(partition)
            w.int16(0)           # error_code
            w.int64(last_offset) # base_offset
            if api_version >= 2:
                w.int64(-1)      # log_append_time_ms
            if api_version >= 5:
                w.int64(-1)      # log_start_offset
    w.int32(0)  # throttle_time_ms
    return frame(correlation_id, w.build())


async def handle_fetch(r: Reader, api_version: int, correlation_id: int) -> bytes:
    _replica_id = r.int32()
    max_wait_ms = r.int32()
    _min_bytes = r.int32()
    if api_version >= 3:
        _max_bytes = r.int32()
    if api_version >= 4:
        _isolation_level = r.int8()
    if api_version >= 7:
        _session_id = r.int32()
        _session_epoch = r.int32()

    topic_count = r.int32()
    fetch_list = []
    for _ in range(topic_count):
        topic = r.string()
        part_count = r.int32()
        parts = []
        for _ in range(part_count):
            partition = r.int32()
            if api_version >= 9:
                _current_leader_epoch = r.int32()
            fetch_offset = r.int64()
            if api_version >= 5:
                _log_start_offset = r.int64()
            max_bytes = r.int32()
            parts.append((partition, fetch_offset, max_bytes))
        fetch_list.append((topic, parts))

    if api_version >= 7:
        _forgotten_count = r.int32()

    # Collect data; if nothing available wait up to max_wait_ms (honours long-poll)
    all_results = []
    has_data = False
    for topic, parts in fetch_list:
        part_results = []
        for partition, fetch_offset, max_bytes in parts:
            messages = BROKER.fetch(topic, partition, fetch_offset, max_bytes)
            if messages:
                has_data = True
            part_results.append((partition, fetch_offset, max_bytes, messages))
        all_results.append((topic, part_results))

    if has_data:
        await asyncio.sleep(FETCH_DATA_DELAY_S)
    elif max_wait_ms > 0:
        await asyncio.sleep(max_wait_ms / 1000.0)

    w = Writer()
    w.int32(0)  # throttle_time_ms
    if api_version >= 7:
        w.int16(0)   # error_code
        w.int32(0)   # session_id
    w.int32(len(all_results))
    for topic, part_results in all_results:
        w.string(topic)
        w.int32(len(part_results))
        for partition, fetch_offset, max_bytes, messages in part_results:
            batch = encode_record_batch(messages) if messages else b""
            w.int32(partition)
            w.int16(0)          # error_code
            w.int64(BROKER.topics[topic][partition].next_offset)  # high_watermark
            if api_version >= 4:
                w.int64(-1)     # last_stable_offset
            if api_version >= 5:
                w.int64(0)      # log_start_offset
            if api_version >= 4:
                w.int32(0)      # aborted_transactions count
            w.int32(len(batch))
            w.raw(batch)
            if messages:
                log.info(f"FETCH  topic={topic} partition={partition} from={fetch_offset} → {len(messages)} msg(s)")
    return frame(correlation_id, w.build())


def handle_list_offsets(r: Reader, api_version: int, correlation_id: int) -> bytes:
    _replica_id = r.int32()
    if api_version >= 2:
        _isolation_level = r.int8()
    topic_count = r.int32()
    results = []
    for _ in range(topic_count):
        topic = r.string()
        part_count = r.int32()
        parts = []
        for _ in range(part_count):
            partition = r.int32()
            if api_version >= 4:
                _current_leader_epoch = r.int32()
            timestamp = r.int64()
            if api_version == 0:
                _max_num = r.int32()
            offset = BROKER.list_offsets(topic, partition, timestamp)
            parts.append((partition, offset))
        results.append((topic, parts))

    w = Writer()
    if api_version >= 2:
        w.int32(0)  # throttle_time_ms
    w.int32(len(results))
    for topic, parts in results:
        w.string(topic)
        w.int32(len(parts))
        for partition, offset in parts:
            w.int32(partition)
            w.int16(0)   # error_code
            if api_version >= 1:
                w.int64(-1)  # timestamp
            w.int64(offset)
            if api_version >= 4:
                w.int32(-1)  # leader_epoch
    return frame(correlation_id, w.build())


def handle_find_coordinator(r: Reader, api_version: int, correlation_id: int) -> bytes:
    if api_version >= 4:
        # v4+: flexible encoding; key_type first, then compact array of keys.
        _key_type = r.int8()
        n_keys = r._varint_u() - 1          # compact array length
        keys = [r.compact_string() for _ in range(n_keys)]
        r._varint_u()                        # body tagged fields
        log.info(f"FindCoordinator v{api_version} key_type={_key_type} keys={keys}")
        w = Writer()
        w.int32(0)                           # throttle_time_ms
        w._varint_u(len(keys) + 1)           # compact array of coordinators
        for key in keys:
            w.compact_string(key)            # key echoed back
            w.int32(1)                       # node_id
            w.compact_string(HOST)           # host
            w.int32(9092)                    # port
            w.int16(0)                       # error_code
            w.compact_string(None)           # error_message (null)
            w._varint_u(0)                   # per-entry tagged fields
        w._varint_u(0)                       # top-level tagged fields
        return frame(correlation_id, w.build(), flexible=True)
    else:
        key = r.string()
        if api_version >= 1:
            _key_type = r.int8()
        log.info(f"FindCoordinator key={key}")
        w = Writer()
        if api_version >= 1:
            w.int32(0)       # throttle_time_ms
        w.int16(0)           # error_code
        if api_version >= 1:
            w.string(None)   # error_message
        w.int32(1)           # node_id
        w.string(HOST)
        w.int32(9092)
        return frame(correlation_id, w.build())


async def _flush_join(group_id: str) -> None:
    """Fire after REBALANCE_TIMEOUT: assign one generation to all pending joiners."""
    waiters = list(BROKER._join_waiters[group_id])
    BROKER._join_waiters[group_id] = []
    BROKER._join_timer.pop(group_id, None)
    if not waiters:
        return

    BROKER.group_generation[group_id] += 1
    gen = BROKER.group_generation[group_id]

    # Fresh sync state for this generation
    BROKER._sync_events[group_id] = asyncio.Event()
    BROKER.groups[group_id].pop("_assignments", None)

    leader = waiters[0]["member_id"]
    BROKER.group_leader[group_id] = leader

    all_members: Dict[str, dict] = {}
    for w in waiters:
        mid = w["member_id"]
        all_members[mid] = {"metadata": w["metadata"]}
        BROKER.groups[group_id][mid] = {
            "protocol": w["protocols"][0][0] if w["protocols"] else "range",
            "metadata": w["metadata"],
        }

    for w in waiters:
        if not w["future"].done():
            w["future"].set_result((gen, leader, all_members))


async def handle_join_group(r: Reader, api_version: int, correlation_id: int) -> bytes:
    log.info(f"JoinGroup body: {r.remaining()} bytes remaining, hex={r.buf[r.pos:].hex()}")
    group_id = r.string()
    _session_timeout = r.int32()
    if api_version >= 1:
        _rebalance_timeout = r.int32()
    member_id = r.string()
    if api_version >= 5:
        _group_instance_id = r.string()
    protocol_type = r.string()
    protocol_count = r.int32()
    protocols = []
    for _ in range(protocol_count):
        name = r.string()
        meta = r.bytes_()
        protocols.append((name, meta))

    if not member_id:
        member_id = str(uuid.uuid4())

    loop = asyncio.get_running_loop()
    fut: asyncio.Future = loop.create_future()
    BROKER._join_waiters[group_id].append({
        "member_id": member_id,
        "protocols": protocols,
        "metadata": protocols[0][1] if protocols else b"",
        "future": fut,
    })

    # Debounce: reset timer so all members joining near-simultaneously share one generation
    old = BROKER._join_timer.pop(group_id, None)
    if old:
        old.cancel()
    BROKER._join_timer[group_id] = loop.call_later(
        BROKER.REBALANCE_TIMEOUT,
        lambda: asyncio.ensure_future(_flush_join(group_id)),
    )

    gen, leader, all_members = await fut
    log.info(f"JoinGroup group={group_id} member={member_id[:8]}... leader={leader[:8]}... gen={gen}")

    w = Writer()
    if api_version >= 2:
        w.int32(0)   # throttle_time_ms
    w.int16(0)       # error_code
    w.int32(gen)     # generation_id
    w.string(protocols[0][0] if protocols else "range")  # protocol_name
    w.string(leader)
    w.string(member_id)
    if member_id == leader:
        w.int32(len(all_members))
        for mid, mdata in all_members.items():
            w.string(mid)
            if api_version >= 5:
                w.string(None)  # group_instance_id
            w.bytes_(mdata["metadata"])
    else:
        w.int32(0)
    return frame(correlation_id, w.build())


async def handle_sync_group(r: Reader, api_version: int, correlation_id: int) -> bytes:
    group_id = r.string()
    generation_id = r.int32()
    member_id = r.string()
    if api_version >= 3:
        _group_instance_id = r.string()
    assignment_count = r.int32()
    assignments = {}
    for _ in range(assignment_count):
        mid = r.string()
        assignment = r.bytes_()
        assignments[mid] = assignment

    log.info(f"SyncGroup group={group_id} member={member_id[:8]}... gen={generation_id}")

    event = BROKER._sync_events.get(group_id)
    if assignments:
        # Leader: store assignments and wake non-leaders
        BROKER.groups[group_id]["_assignments"] = assignments
        if event:
            event.set()
    elif event:
        # Non-leader: wait up to 30 s for leader to send assignments
        try:
            await asyncio.wait_for(event.wait(), timeout=30.0)
        except asyncio.TimeoutError:
            pass

    my_assignment = BROKER.groups[group_id].get("_assignments", {}).get(member_id, b"")

    w = Writer()
    if api_version >= 1:
        w.int32(0)   # throttle_time_ms
    w.int16(0)       # error_code
    w.bytes_(my_assignment)
    return frame(correlation_id, w.build())


def handle_heartbeat(r: Reader, api_version: int, correlation_id: int) -> bytes:
    group_id = r.string()
    generation_id = r.int32()
    member_id = r.string()
    w = Writer()
    if api_version >= 1:
        w.int32(0)   # throttle_time_ms
    w.int16(0)       # error_code
    return frame(correlation_id, w.build())


def handle_leave_group(r: Reader, api_version: int, correlation_id: int) -> bytes:
    group_id = r.string()
    member_id = r.string()
    group = BROKER.groups.get(group_id, {})
    group.pop(member_id, None)
    log.info(f"LeaveGroup group={group_id} member={member_id[:8] if member_id else '?'}...")
    w = Writer()
    if api_version >= 1:
        w.int32(0)   # throttle_time_ms
    w.int16(0)       # error_code
    return frame(correlation_id, w.build())


def handle_offset_commit(r: Reader, api_version: int, correlation_id: int) -> bytes:
    group_id = r.string()
    if api_version >= 1:
        _generation_id = r.int32()
        _member_id = r.string()
    if api_version >= 7:
        _group_instance_id = r.string()
    topic_count = r.int32()
    results = []
    for _ in range(topic_count):
        topic = r.string()
        part_count = r.int32()
        parts = []
        for _ in range(part_count):
            partition = r.int32()
            committed_offset = r.int64()
            if api_version >= 6:
                _leader_epoch = r.int32()
            _metadata = r.string()
            BROKER.commit_offset(group_id, topic, partition, committed_offset)
            parts.append(partition)
        results.append((topic, parts))
    log.info(f"OffsetCommit group={group_id} topics={[t for t, _ in results]}")
    w = Writer()
    if api_version >= 3:
        w.int32(0)  # throttle_time_ms
    w.int32(len(results))
    for topic, parts in results:
        w.string(topic)
        w.int32(len(parts))
        for partition in parts:
            w.int32(partition)
            w.int16(0)  # error_code
    return frame(correlation_id, w.build())


def handle_offset_fetch(r: Reader, api_version: int, correlation_id: int) -> bytes:
    group_id = r.string()
    topic_count = r.int32()
    results = []
    for _ in range(topic_count):
        topic = r.string()
        part_count = r.int32()
        parts = []
        for _ in range(part_count):
            partition = r.int32()
            offset = BROKER.get_offset(group_id, topic, partition)
            parts.append((partition, offset))
        results.append((topic, parts))

    w = Writer()
    if api_version >= 3:
        w.int32(0)   # throttle_time_ms
    w.int32(len(results))
    for topic, parts in results:
        w.string(topic)
        w.int32(len(parts))
        for partition, offset in parts:
            w.int32(partition)
            w.int64(offset)
            if api_version >= 5:
                w.int32(-1)      # leader_epoch
            w.string(None)       # metadata
            w.int16(0)           # error_code
    if api_version >= 2:
        w.int16(0)   # error_code
    return frame(correlation_id, w.build())


def handle_init_producer_id(r: Reader, api_version: int, correlation_id: int) -> bytes:
    # kafka-clients 3.x enables idempotent producers by default, which requires
    # this API. Return a fixed producer_id so the client can proceed without
    # transactions.
    # v3+ uses flexible encoding: compact strings + tagged fields throughout.
    if api_version >= 3:
        _transactional_id = r.compact_string()
        _transaction_timeout_ms = r.int32()
        _producer_id = r.int64()
        _producer_epoch = r.int16()
        r._varint_u()  # request tagged fields
        w = Writer()
        w.int32(0)    # throttle_time_ms
        w.int16(0)    # error_code
        w.int64(1)    # producer_id
        w.int16(0)    # producer_epoch
        w._varint_u(0)  # response tagged fields
        return frame(correlation_id, w.build(), flexible=True)
    else:
        _transactional_id = r.string()
        _transaction_timeout_ms = r.int32()
        w = Writer()
        w.int32(0)    # throttle_time_ms
        w.int16(0)    # error_code
        w.int64(1)    # producer_id
        w.int16(0)    # producer_epoch
        return frame(correlation_id, w.build())


HANDLERS = {
    0:  handle_produce,
    1:  handle_fetch,
    2:  handle_list_offsets,
    3:  handle_metadata,
    8:  handle_offset_commit,
    9:  handle_offset_fetch,
    10: handle_find_coordinator,
    11: handle_join_group,
    12: handle_heartbeat,
    13: handle_leave_group,
    14: handle_sync_group,
    18: handle_api_versions,
    22: handle_init_producer_id,
}

API_NAMES = {
    0: "Produce", 1: "Fetch", 2: "ListOffsets", 3: "Metadata",
    8: "OffsetCommit", 9: "OffsetFetch", 10: "FindCoordinator", 11: "JoinGroup",
    12: "Heartbeat", 13: "LeaveGroup", 14: "SyncGroup", 18: "ApiVersions",
    22: "InitProducerId",
}


# ---------------------------------------------------------------------------
# Async TCP server
# ---------------------------------------------------------------------------

async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    peer = writer.get_extra_info("peername")
    log.info(f"Client connected: {peer}")
    try:
        while True:
            # Read 4-byte length prefix
            size_bytes = await reader.readexactly(4)
            size = struct.unpack(">i", size_bytes)[0]
            log.debug(f"  raw size_bytes={size_bytes.hex()} parsed size={size}")
            if size <= 0:
                log.warning(f"  Closing: non-positive frame size {size}")
                break
            payload = await reader.readexactly(size)
            log.debug(f"  raw payload[0:20]={payload[:20].hex()}")

            r = Reader(payload)
            api_key = r.int16()
            api_version = r.int16()
            correlation_id = r.int32()
            # Request Header v2 (flexible): same NULLABLE_STRING client_id as v1,
            # plus a trailing tagged-fields varint (TAG_BUFFER).
            # Request Header v1 (standard): int16-length client_id string only.
            # Flexible header thresholds: ApiVersions v3+, FindCoordinator v4+,
            # JoinGroup v6+, Heartbeat/LeaveGroup/SyncGroup v4+.
            _FLEXIBLE_HDR = {18: 3, 10: 4, 11: 6, 12: 4, 13: 4, 14: 4}
            client_id = r.string()
            if api_version >= _FLEXIBLE_HDR.get(api_key, 999):
                r._varint_u()  # skip tagged fields (header v2 only)

            api_name = API_NAMES.get(api_key, f"Unknown({api_key})")
            log.debug(f"← {api_name} v{api_version} corr={correlation_id} client={client_id}")

            handler = HANDLERS.get(api_key)
            if handler:
                try:
                    if inspect.iscoroutinefunction(handler):
                        response = await handler(r, api_version, correlation_id)
                    else:
                        response = handler(r, api_version, correlation_id)
                    writer.write(response)
                    await writer.drain()
                except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                    return
                except OSError as e:
                    if e.errno in (64, 10054, 10053, 32):
                        return
                    log.error(f"Error handling {api_name}: {e}", exc_info=True)
                    return
                except Exception as e:
                    log.error(f"Error handling {api_name}: {e}", exc_info=True)
                    try:
                        w = Writer().int16(35)  # UNKNOWN_SERVER_ERROR
                        writer.write(frame(correlation_id, w.build()))
                        await writer.drain()
                    except OSError:
                        return
            else:
                log.warning(f"Unhandled API key {api_key} ({api_name})")
                # Return empty response to avoid client hang
                writer.write(frame(correlation_id, b""))
                await writer.drain()

    except (asyncio.IncompleteReadError, ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
        pass
    except OSError as e:
        if e.errno in (64, 10054, 10053, 32):  # WinError 64, WSAECONNRESET, WSAECONNABORTED, EPIPE
            pass
        else:
            log.error(f"Client {peer} error: {e}", exc_info=True)
    except Exception as e:
        log.error(f"Client {peer} error: {e}", exc_info=True)
    finally:
        log.info(f"Client disconnected: {peer}")
        writer.close()
        try:
            await writer.wait_closed()
        except Exception:
            pass


async def main(host: str, port: int):
    global HOST
    HOST = host
    server = await asyncio.start_server(handle_client, host, port)
    addrs = ", ".join(str(s.getsockname()) for s in server.sockets)
    log.info(f"Kafka emulator listening on {addrs}")
    log.info("Point your Java services at: bootstrap.servers=%s:%d", host, port)
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Kafka Protocol Emulator")
    parser.add_argument("--host", default="localhost", help="Bind host (default: localhost)")
    parser.add_argument("--port", type=int, default=9092, help="Bind port (default: 9092)")
    parser.add_argument("--debug", action="store_true", help="Enable DEBUG logging (raw frames, all requests)")
    args = parser.parse_args()
    if args.debug:
        logging.getLogger().setLevel(logging.DEBUG)
    asyncio.run(main(args.host, args.port))
