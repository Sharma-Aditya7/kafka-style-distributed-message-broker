# YAK Message Broker - Failover Demo Guide

## Objective

Demonstrate that the system can survive a catastrophic leader failure with **ZERO DATA LOSS**.

## Demo Scenario

1. Send 100 messages to the leader
2. Kill the leader broker process
3. Follower automatically detects failure and becomes new leader
4. Producer and Consumer automatically reconnect to new leader
5. Consumer reads all 100 messages → Proof of zero data loss!

## Prerequisites

- All 4 nodes are set up and running (see [SETUP.md](SETUP.md))
- Redis is running on Node 2
- Leader Broker is running on Node 1
- Follower Broker is running on Node 2

## Demo Steps

### Step 1: Send 100 Messages to Leader

**On Node 3 (Producer):**

```bash
# Start producer in batch mode
python -m producer.producer \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --batch 100
```

**Expected Output:**
```
Discovered leader: <LEADER_IP>:9092
✓ Connected to leader at <LEADER_IP>:9092
Sent message: Test message 1
✓ Message acknowledged at offset 0
Sent message: Test message 2
✓ Message acknowledged at offset 1
...
Sent message: Test message 100
✓ Message acknowledged at offset 99

Results: 100 success, 0 failed
```

**What to Observe:**
- ✅ All 100 messages successfully sent
- ✅ Each message receives an acknowledgment
- ✅ Producer connected to Leader on Node 1

---

### Step 2: Verify Messages Are Replicated

**On Node 1 (Leader) logs:**
Look for replication confirmations:
```
Sent replication request for offset 0
Received ACK for offset 0
HWM updated to 0
✓ Message committed at offset 0
...
HWM updated to 99
✓ Message committed at offset 99
```

**On Node 2 (Follower) logs:**
Look for replication receipts:
```
✓ Replicated message at offset 0
✓ Replicated message at offset 1
...
✓ Replicated message at offset 99
```

**What to Observe:**
- ✅ Follower logs show all 100 messages replicated
- ✅ Leader logs show ACKs from follower
- ✅ HWM (High Water Mark) updated to 99

---

### Step 3: Kill the Leader Broker

**On Node 1:**

Find the leader process:
```bash
# Linux/Mac:
ps aux | grep leader_broker

# Windows:
tasklist | findstr python
```

Kill it:
```bash
# Linux/Mac:
kill -9 <PID>

# Windows:
taskkill /F /PID <PID>

# Or simply press Ctrl+C in the leader terminal
```

**What to Observe:**
- ✅ Leader broker process terminates
- ✅ No more lease renewals in Redis

---

### Step 4: Follower Detects Failure and Promotes Itself

**On Node 2 (Follower) logs:**

You should see (within ~15-20 seconds):
```
⚠ Leader lease not found (attempt 1/3)
⚠ Leader lease not found (attempt 2/3)
⚠ Leader lease not found (attempt 3/3)
Leader failure detected! Attempting election...
Attempting to acquire leadership...
✓✓✓ LEADERSHIP ACQUIRED! ✓✓✓
New leader: <FOLLOWER_IP>:9093
🎉 PROMOTION TO LEADER! 🎉
✓ Now acting as leader - accepting producer requests
Leader lease renewed (TTL: 30s)
```

**Check Redis to Confirm:**
```bash
redis-cli -h <REDIS_IP> GET leader:current
# Should show: {"host":"<FOLLOWER_IP>","port":9093}
```

**What to Observe:**
- ✅ Follower detects missing heartbeat (3 consecutive failures)
- ✅ Follower atomically acquires leadership via Redis SETNX
- ✅ Follower starts acting as leader
- ✅ Leader metadata in Redis updated

**⏱ Downtime:** ~15-20 seconds (configurable via HEARTBEAT_TIMEOUT)

---

### Step 5: Producer Reconnects to New Leader

**On Node 3 (Producer):**

Try sending a new message:
```bash
python -m producer.producer \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --message "Message after failover"
```

**Expected Output:**
```
Failed to connect to leader: [Errno 111] Connection refused
⚠ Broker is not the leader - discovering new leader...
Discovered leader: <FOLLOWER_IP>:9093
✓ Connected to leader at <FOLLOWER_IP>:9093
Sent message: Message after failover
✓ Message acknowledged at offset 100
```

**What to Observe:**
- ✅ Producer detects old leader is down
- ✅ Producer queries Redis for new leader
- ✅ Producer connects to new leader (Follower promoted)
- ✅ Message successfully sent to new leader

---

### Step 6: Consumer Reads All Messages

**On Node 4 (Consumer):**

Fetch all messages from the beginning:
```bash
python -m consumer.consumer \
    --consumer-id "demo-consumer" \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --fetch-all
```

**Expected Output:**
```
Loaded offset: -1
Discovered leader: <FOLLOWER_IP>:9093
✓ Connected to leader at <FOLLOWER_IP>:9093
Fetching messages from offset 0...
✓ Received 100 messages (HWM: 99)
[Offset 0] Test message 1
[Offset 1] Test message 2
[Offset 2] Test message 3
...
[Offset 99] Test message 100
✓ Fetched 100 total messages
```

**Count the messages:**
```bash
python -m consumer.consumer \
    --consumer-id "demo-consumer" \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --fetch-all | grep "Offset" | wc -l

# Should output: 100
```

**What to Observe:**
- ✅ Consumer discovers new leader
- ✅ Consumer fetches all 100 messages
- ✅ **ZERO DATA LOSS** - All messages survived the crash!

---

## Success Criteria

### ✅ Checklist:

- [ ] 100 messages sent successfully to initial leader
- [ ] All 100 messages replicated to follower (check logs)
- [ ] Leader crashed (process killed)
- [ ] Follower detected failure within 15-20 seconds
- [ ] Follower promoted itself to leader
- [ ] Producer automatically reconnected to new leader
- [ ] Consumer automatically connected to new leader
- [ ] Consumer fetched exactly 100 messages
- [ ] **ZERO DATA LOSS CONFIRMED**

---

## Demo Metrics

| Metric | Value |
|--------|-------|
| Messages Sent | 100 |
| Messages Lost | 0 |
| Data Loss | **0%** |
| Failover Time | ~15-20 seconds |
| Producer Reconnect | Automatic |
| Consumer Reconnect | Automatic |

---

## Understanding What Happened

### 1. Synchronous Replication
- Leader didn't ACK producer until follower confirmed replication
- This guarantees all ACK'd messages are on both brokers

### 2. High Water Mark (HWM)
- HWM = highest offset replicated to follower
- Consumers can only read up to HWM
- Ensures no uncommitted data is consumed

### 3. Leader Election
- Follower monitors leader's heartbeat (every 5 seconds)
- After 3 missed heartbeats (15 seconds), declares leader dead
- Uses Redis SETNX for atomic leader election
- Only one follower can become leader (prevents split-brain)

### 4. Client Failover
- Producer/Consumer store list of all broker addresses
- On connection error, query Redis for current leader
- Automatically reconnect to new leader
- Retry failed operations with new leader

---

## Advanced Demo: Multiple Failures

### Test Consumer Offset Persistence

1. Start consumer and read 50 messages:
   ```bash
   # Read and commit offsets
   python -m consumer.consumer --fetch-all
   ```

2. Kill the consumer (Ctrl+C)

3. Kill the current leader

4. Restart consumer:
   ```bash
   # Should resume from offset 50, not 0
   python -m consumer.consumer --continuous
   ```

**Expected:** Consumer resumes from last committed offset, even after leader failover!

---

## Troubleshooting Demo

### Issue: Follower doesn't promote itself

**Possible Causes:**
- Heartbeat timeout too short
- Redis connection issue
- Follower not monitoring leader

**Debug:**
```bash
# Check if leader lease exists in Redis
redis-cli -h <REDIS_IP> GET leader:lease

# Check follower logs for monitoring messages
# Should see: "Leader alive: ..." or "Leader lease not found"
```

### Issue: Consumer can't read messages after failover

**Possible Causes:**
- Consumer not discovering new leader
- HWM not updated

**Debug:**
```bash
# Check current leader in Redis
redis-cli -h <REDIS_IP> GET leader:current

# Check HWM
redis-cli -h <REDIS_IP> GET hwm:offset
```

### Issue: Messages missing after failover

**This should NEVER happen!**

If it does, check:
1. Did leader wait for follower ACK before ACKing producer?
2. Check replication logs - were all messages replicated?
3. Check HWM - was it updated correctly?

---

## Video Demo Script (for Presentation)

1. **Show initial state** (30 sec)
   - All 4 terminals visible
   - Leader and Follower running
   - Redis running

2. **Send messages** (1 min)
   - Producer terminal: Send 100 messages
   - Show progress and ACKs
   - Highlight replication logs

3. **Kill leader** (30 sec)
   - Dramatically kill leader process
   - Show follower detecting failure
   - Show follower promotion

4. **Show recovery** (1 min)
   - Send new message from producer
   - Show automatic reconnection
   - Consumer fetches all 100 messages

5. **Proof of zero data loss** (30 sec)
   - Count messages: 100 sent, 100 received
   - Show metrics and success

**Total: ~4 minutes**

---

## Congratulations! 🎉

You've successfully demonstrated a fault-tolerant distributed message broker with:
- ✅ Synchronous replication
- ✅ Automatic leader election
- ✅ Client failover
- ✅ Zero data loss
- ✅ High availability

This is the core of how real distributed systems like Apache Kafka work!
