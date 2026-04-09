# kafka-emulator

Single-file, zero-dependency Kafka protocol emulator over raw TCP.  
Drop-in replacement for local development — no Kafka, no Docker, no ZooKeeper.

## Requirements

Python 3.10+ standard library only.

## Start

```bash
python kafka_emulator.py
```

```bash
# custom host/port
python kafka_emulator.py --host 0.0.0.0 --port 9092

# verbose output (raw frames + all requests)
python kafka_emulator.py --debug
```

## Limitations

- **No SSL/TLS.** Plaintext only.

## Connect

Point your client at `localhost:9092`.  
SSL must be disabled on the client side.

**Spring Boot / application.properties:**
```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.security.protocol=PLAINTEXT
spring.kafka.properties.security.protocol=PLAINTEXT
```

**Java:**
```java
props.put("bootstrap.servers", "localhost:9092");
```

## Behaviour

- Topics are **auto-created** on first Produce or Metadata request.
- Messages are stored **in-memory** — state is lost on restart.
- Consumer groups are supported: JoinGroup / SyncGroup / Heartbeat / LeaveGroup.
- `FETCH_DATA_DELAY_S` (top of file, default `5.0`) controls the delay added to Fetch responses that contain data — prevents tight retry loops.

## Supported APIs

| Key | Name            |
|-----|-----------------|
| 0   | Produce         |
| 1   | Fetch           |
| 2   | ListOffsets     |
| 3   | Metadata        |
| 8   | OffsetCommit    |
| 9   | OffsetFetch     |
| 10  | FindCoordinator |
| 11  | JoinGroup       |
| 12  | Heartbeat       |
| 13  | LeaveGroup      |
| 14  | SyncGroup       |
| 18  | ApiVersions     |
| 22  | InitProducerId  |
