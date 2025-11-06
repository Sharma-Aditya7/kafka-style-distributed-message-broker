# YAK Architecture Documentation

## System Overview

YAK is a distributed message broker implementing core Kafka-like functionality with a leader-follower replication model.

## High-Level Architecture

```
                    ┌─────────────────────────────────┐
                    │     Distributed System          │
                    │                                 │
                    │   ┌──────────┐  ┌──────────┐   │
                    │   │  Node 1  │  │  Node 2  │   │
                    │   │  Leader  │◄─┤ Follower │   │
                    │   │  Broker  │──┤  Broker  │   │
                    │   └────┬─────┘  └────┬─────┘   │
                    │        │             │         │
                    │        │   ┌─────────▼──────┐  │
                    │        │   │  Redis Store   │  │
                    │        │   │  (Metadata)    │  │
                    │        │   └────────────────┘  │
                    │        │                       │
                    │   ┌────▼────┐   ┌────▼────┐   │
                    │   │ Node 3  │   │ Node 4  │   │
                    │   │Producer │   │Consumer │   │
                    │   └─────────┘   └─────────┘   │
                    │                                 │
                    └─────────────────────────────────┘
```

## Component Architecture

### 1. Leader Broker (Node 1)

```
┌─────────────────────────────────────────┐
│         Leader Broker Process           │
├─────────────────────────────────────────┤
│                                         │
│  ┌───────────────────────────────────┐ │
│  │     TCP Server (Port 9092)        │ │
│  │  - Accept producer connections    │ │
│  │  - Handle PRODUCE requests         │ │
│  │  - Handle FETCH requests           │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │      Log Manager                  │ │
│  │  - In-memory message storage      │ │
│  │  - Thread-safe operations         │ │
│  │  - Message deduplication          │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │   Replication Manager             │ │
│  │  - TCP client to follower         │ │
│  │  - Synchronous replication        │ │
│  │  - Wait for ACK before commit     │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │   Leader Lease Manager            │ │
│  │  - Renew lease every 5 seconds    │ │
│  │  - Update HWM in Redis            │ │
│  │  - Maintain leadership            │ │
│  └───────────────────────────────────┘ │
│                                         │
└─────────────────────────────────────────┘
```

**Key Responsibilities:**
- Accept write requests from producers
- Replicate synchronously to follower
- Update High Water Mark after replication
- Maintain leader lease in Redis
- Serve read requests to consumers

**Critical Code Paths:**

1. **Write Path** (zero data loss guarantee):
   ```
   Producer Request → Append to Local Log → Replicate to Follower
   → Wait for Follower ACK → Update HWM in Redis
   → Send ACK to Producer
   ```

2. **Read Path** (consistency guarantee):
   ```
   Consumer Request → Get HWM from Redis
   → Fetch messages from Log (up to HWM)
   → Return to Consumer
   ```

---

### 2. Follower Broker (Node 2)

```
┌─────────────────────────────────────────┐
│        Follower Broker Process          │
├─────────────────────────────────────────┤
│                                         │
│  ┌───────────────────────────────────┐ │
│  │     TCP Server (Port 9093)        │ │
│  │  - Accept replication requests    │ │
│  │  - Reject producer writes         │ │
│  │  - Handle metadata queries        │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │      Log Manager                  │ │
│  │  - Replica of leader's log        │ │
│  │  - Identical message storage      │ │
│  │  - Ready to become leader         │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │   Election Manager                │ │
│  │  - Monitor leader heartbeat       │ │
│  │  - Detect leader failure          │ │
│  │  - Atomic leader election         │ │
│  │  - Promote to leader on failure   │ │
│  └───────────────────────────────────┘ │
│                                         │
└─────────────────────────────────────────┘
```

**Key Responsibilities:**
- Receive and store replicated data
- Monitor leader health via Redis lease
- Detect leader failures (3 missed heartbeats)
- Atomically acquire leadership on failure
- Become new leader seamlessly

**Critical Code Paths:**

1. **Replication Path**:
   ```
   Receive REPLICATE → Append to Local Log
   → Send ACK to Leader
   ```

2. **Failover Path**:
   ```
   Monitor Leader Lease → Detect Expiration (3 checks)
   → Attempt Redis SETNX → Acquire Leadership
   → Start Acting as Leader
   ```

---

### 3. Redis Metadata Store

```
┌─────────────────────────────────────────┐
│          Redis Key-Value Store          │
├─────────────────────────────────────────┤
│                                         │
│  Key: "leader:lease"                    │
│  Value: {"host": "x.x.x.x", "port": N}  │
│  TTL: 30 seconds                        │
│  Purpose: Leader heartbeat              │
│                                         │
│  Key: "leader:current"                  │
│  Value: {"host": "x.x.x.x", "port": N}  │
│  TTL: None (persistent)                 │
│  Purpose: Current leader info           │
│                                         │
│  Key: "hwm:offset"                      │
│  Value: 99 (integer)                    │
│  TTL: None                              │
│  Purpose: High Water Mark               │
│                                         │
│  Key: "consumer:offset:<consumer-id>"   │
│  Value: 42 (integer)                    │
│  TTL: None                              │
│  Purpose: Consumer position tracking    │
│                                         │
└─────────────────────────────────────────┘
```

**Key Operations:**

1. **Leader Election (Atomic)**:
   ```
   SETNX leader:lease {"host": "x.x.x.x", "port": 9093}
   EXPIRE leader:lease 30
   ```
   - Only one broker can successfully SET
   - Prevents split-brain

2. **Leader Renewal**:
   ```
   SET leader:lease {"host": "x.x.x.x", "port": 9092} EX 30
   ```
   - Leader updates every 5 seconds
   - TTL ensures expiration if leader dies

3. **Metadata Query**:
   ```
   GET leader:current
   → {"host": "192.168.1.101", "port": 9092}
   ```

---

### 4. Producer Client (Node 3)

```
┌─────────────────────────────────────────┐
│         Producer Client                 │
├─────────────────────────────────────────┤
│                                         │
│  ┌───────────────────────────────────┐ │
│  │   Leader Discovery Module         │ │
│  │  - Query Redis for leader         │ │
│  │  - Fallback: query brokers        │ │
│  │  - Cache leader connection        │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │   Message Sender                  │ │
│  │  - Generate UUID for each message │ │
│  │  - Send to leader via TCP         │ │
│  │  - Wait for ACK with offset       │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │   Failover Handler                │ │
│  │  - Detect connection errors       │ │
│  │  - Rediscover leader              │ │
│  │  - Retry with same UUID           │ │
│  │  - Exponential backoff            │ │
│  └───────────────────────────────────┘ │
│                                         │
└─────────────────────────────────────────┘
```

**Intelligence Features:**
- Automatic leader discovery
- Transparent failover
- Message deduplication
- Retry with backoff

---

### 5. Consumer Client (Node 4)

```
┌─────────────────────────────────────────┐
│         Consumer Client                 │
├─────────────────────────────────────────┤
│                                         │
│  ┌───────────────────────────────────┐ │
│  │   Leader Discovery Module         │ │
│  │  - Query Redis for leader         │ │
│  │  - Reconnect on failure           │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │   Offset Manager                  │ │
│  │  - Track current offset           │ │
│  │  - Commit to Redis after process  │ │
│  │  - Resume from last committed     │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │   Message Fetcher                 │ │
│  │  - Request from current offset    │ │
│  │  - Respect HWM boundary           │ │
│  │  - Process and commit offset      │ │
│  └──────────┬────────────────────────┘ │
│             │                           │
│  ┌──────────▼────────────────────────┐ │
│  │   Failover Handler                │ │
│  │  - Detect leader failure          │ │
│  │  - Rediscover new leader          │ │
│  │  - Resume from committed offset   │ │
│  └───────────────────────────────────┘ │
│                                         │
└─────────────────────────────────────────┘
```

**Consistency Guarantees:**
- Only reads committed data (up to HWM)
- Offset persistence across failures
- Exactly-once processing semantics

---

## Data Flow Diagrams

### Normal Operation (All Nodes Healthy)

```
Producer                Leader              Follower            Consumer
   │                      │                    │                   │
   │  1. PRODUCE msg      │                    │                   │
   ├─────────────────────►│                    │                   │
   │                      │  2. REPLICATE msg  │                   │
   │                      ├───────────────────►│                   │
   │                      │                    │  3. Write to log  │
   │                      │                    │                   │
   │                      │  4. ACK            │                   │
   │                      │◄───────────────────┤                   │
   │                      │  5. Update HWM     │                   │
   │                      │       in Redis     │                   │
   │  6. ACK (offset=N)   │                    │                   │
   │◄─────────────────────┤                    │                   │
   │                      │                    │                   │
   │                      │                    │  7. FETCH         │
   │                      │                    │◄──────────────────┤
   │                      │                    │  8. Return msgs   │
   │                      │                    │  (up to HWM)      │
   │                      │                    ├──────────────────►│
```

**Key Points:**
- Step 2-4: Synchronous replication (guarantees durability)
- Step 5: HWM update (makes data readable)
- Step 6: Only after replication does producer get ACK
- Step 8: Consumer only reads up to HWM

---

### Failover Scenario (Leader Crashes)

```
Producer     Leader      Follower    Redis       Consumer
   │           │            │          │            │
   │  PRODUCE  │            │          │            │
   ├──────────►│            │          │            │
   │           X (CRASH!)   │          │            │
   │         Connection     │          │            │
   │           Lost!        │          │            │
   │                        │          │            │
   │                        │ No lease │            │
   │                        │ renewal  │            │
   │                        ├─────────►│            │
   │                        │  (3x)    │            │
   │                        │          │            │
   │                        │ SETNX    │            │
   │                        │ (atomic) │            │
   │                        ├─────────►│            │
   │                        │  Success!│            │
   │                        │◄─────────┤            │
   │                        │          │            │
   │   Query leader?        │ I'm the  │            │
   ├────────────────────────┼─────────►│            │
   │                        │  leader! │            │
   │◄───────────────────────┼──────────┤            │
   │                        │          │            │
   │  PRODUCE (retry)       │          │            │
   ├───────────────────────►│          │            │
   │  ACK                   │          │            │
   │◄───────────────────────┤          │            │
   │                        │          │            │
   │                        │          │  FETCH     │
   │                        │          │◄───────────┤
   │                        │          │  Messages  │
   │                        │          ├───────────►│
```

**Failover Steps:**
1. Leader crashes
2. Producer detects connection loss
3. Follower detects missing heartbeat (15s)
4. Follower acquires leadership via Redis SETNX
5. Producer queries Redis for new leader
6. Producer connects to new leader (follower)
7. Operations resume normally

**Downtime:** ~15-20 seconds (configurable)

---

## Message Format

### Wire Protocol (JSON over TCP)

```json
{
  "type": "PRODUCE|REPLICATE|FETCH|ACK|METADATA",
  "data": "message content or metadata",
  "offset": 42,
  "timestamp": 1699272000.123,
  "message_id": "uuid-string"
}
```

**Frame Format:**
```
[4 bytes: length (big-endian)][N bytes: JSON message]
```

Example:
```
[0x00 0x00 0x00 0x50][{"type":"PRODUCE","data":"Hello","message_id":"..."}]
```

---

## Fault Tolerance Mechanisms

### 1. Zero Data Loss Guarantee

```
┌─────────────────────────────────────────┐
│  Message Lifecycle                      │
├─────────────────────────────────────────┤
│                                         │
│  1. Producer sends message              │
│  2. Leader writes to local log          │
│  3. Leader sends to follower            │
│  4. Follower writes to log     ←─┐     │
│  5. Follower sends ACK           │     │
│  6. Leader updates HWM           │ Synchronous
│  7. Leader sends ACK to producer │ (Blocks here)
│                                  └─┘   │
│  Only now is message "committed"        │
│                                         │
└─────────────────────────────────────────┘
```

**Why This Works:**
- Leader only ACKs after follower confirms
- If leader crashes before ACK, producer retries
- If leader crashes after ACK, data is on both nodes
- New leader has all committed data

### 2. Split-Brain Prevention

```
Scenario: Network partition

    Follower 1          Follower 2
        │                   │
        │                   │
        ├──► Redis SETNX    │
        │    "leader:lease" │
        │    Returns: OK   │
        │                   │
        │                   ├──► Redis SETNX
        │                   │    "leader:lease"
        │                   │    Returns: nil
        │                   │    (Key exists!)
        ▼                   ▼
    Becomes Leader    Stays Follower
```

**Atomicity Guarantee:**
- Redis SETNX is atomic
- Only one operation succeeds
- Impossible for both to become leader

### 3. Read-After-Write Consistency

```
Producer writes offset 100
    │
    ▼
Leader replicates to follower
    │
    ▼
Leader updates HWM to 100
    │
    ▼
Consumer can now read offset 100
```

**Before HWM Update:**
- Message at offset 100 exists on leader
- But HWM is still 99
- Consumer fetch returns up to offset 99
- Consumer doesn't see offset 100 yet

**After HWM Update:**
- HWM is now 100
- Message is "committed" (replicated)
- Consumer can safely read offset 100
- Even if leader crashes, message persists

---

## Performance Characteristics

### Latency Breakdown

```
Producer Send Latency = Network + Replication + Disk (if enabled)

Normal case:
├─ Network to Leader: 1-5ms
├─ Leader log append: <1ms
├─ Replication to Follower: 5-20ms
│  ├─ Network: 1-5ms
│  ├─ Follower log append: <1ms
│  └─ ACK return: 1-5ms
├─ HWM update in Redis: 1-5ms
└─ ACK to Producer: 1-5ms

Total: 10-50ms per message
```

### Throughput

```
Single-threaded: ~100-500 msg/sec
Bottleneck: Synchronous replication

Potential Optimizations:
- Batch replication (multiple messages per round-trip)
- Pipelining (send next while waiting for ACK)
- Multiple partitions (parallel processing)
```

### Scalability Limits

```
Current Architecture:
- Single leader (write bottleneck)
- Single follower (no redundancy beyond 1 copy)
- No partitioning (can't scale horizontally)

Production Kafka:
- Multiple partitions (parallel writes)
- Multiple replicas per partition
- Horizontal scaling
```

---

## Security Considerations

**Current Implementation:**
- ⚠️ No authentication
- ⚠️ No encryption (plain TCP)
- ⚠️ No authorization

**Production Requirements:**
- TLS for wire encryption
- SASL for authentication
- ACLs for authorization
- Network isolation (VPC)

---

## Comparison with Apache Kafka

| Feature | YAK | Apache Kafka |
|---------|-----|--------------|
| Replication | Synchronous (1 follower) | Configurable (N replicas) |
| Partitions | 1 (single topic) | Multiple (horizontal scaling) |
| Consensus | Redis SETNX | ZooKeeper/KRaft |
| Language | Python | Java/Scala |
| Persistence | In-memory | Disk-based logs |
| Performance | 100-500 msg/s | 100K+ msg/s |
| Client Protocol | JSON/TCP | Binary protocol |
| Message Retention | No limit (memory) | Time/size based |
| Consumer Groups | Single consumer | Multiple consumers/group |

---

## Future Enhancements

1. **Multiple Partitions**
   - Horizontal scaling
   - Parallel processing

2. **Disk Persistence**
   - Survive broker restarts
   - Larger message storage

3. **Consumer Groups**
   - Multiple consumers per topic
   - Load balancing

4. **Batch Operations**
   - Higher throughput
   - Reduced latency

5. **Compression**
   - Reduce network bandwidth
   - Faster replication

6. **Security**
   - TLS encryption
   - Authentication/Authorization

---

This architecture demonstrates the core principles of distributed message brokers while remaining simple enough to understand and implement in an academic setting.
