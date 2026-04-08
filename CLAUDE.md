# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the emulator

```bash
python kafka_emulator.py                        # defaults: localhost:9092
python kafka_emulator.py --host 0.0.0.0 --port 9092
```

No dependencies beyond the Python standard library.

## Architecture

The entire emulator is a single file (`kafka_emulator.py`) implementing a subset of the Kafka binary wire protocol over raw TCP.

**Request lifecycle** (`handle_client`): each TCP connection is handled by an async coroutine that reads length-prefixed frames, parses the 4-field request header (api_key, api_version, correlation_id, client_id), dispatches to a handler via `HANDLERS`, and writes the framed response back.

**Protocol encoding**: `Reader`/`Writer` classes wrap `struct.pack/unpack` for the Kafka binary types. Two encoding styles are used:
- Standard (most requests): fixed-width int16/int32/int64, length-prefixed strings/bytes.
- Flexible/compact (NOT implemented): varint-encoded lengths + tagged fields, used by Kafka ≥ v9 for most APIs.

**Version cap rule**: `handle_api_versions` advertises max versions just below where each API switches to flexible encoding. This is intentional — do not raise these caps without implementing compact encoding in the corresponding handler. The thresholds are: Produce v9+, Fetch v12+, ListOffsets v6+, Metadata v9+, OffsetFetch v8+, FindCoordinator v4+, JoinGroup v6+, Heartbeat/LeaveGroup/SyncGroup v4+, ApiVersions v3+.

**In-memory state** (`Broker` singleton): topics → partitions → `TopicPartition` (list of `Message`). Topics are auto-created on first Metadata or Produce request. State is lost on restart.

**Record batches**: Kafka v2 record batch format is fully implemented for both encode (`encode_record_batch`) and decode (`decode_record_batch`). Legacy magic 0/1 batches are decoded best-effort only.

**Consumer groups**: JoinGroup/SyncGroup/Heartbeat/LeaveGroup are implemented synchronously (no async rebalance barrier). The first member to join becomes the leader and is responsible for sending partition assignments in SyncGroup.
