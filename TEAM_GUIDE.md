# Team Member Assignment Guide

## Overview

This document provides specific instructions for each of the 4 team members on what they need to do.

---

## 👤 Person 1: Leader Broker (Node 1)

### Your Role
You're responsible for the **Leader Broker** - the primary node that accepts all write requests from producers.

### Your Machine Setup

1. **Get your machine's IP address:**
   ```bash
   # Linux/Mac
   ip addr show

   # Windows
   ipconfig
   ```

2. **Install dependencies:**
   ```bash
   cd airline-kafka-pipeline
   python3 -m venv venv
   source venv/bin/activate  # On Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. **Update configuration:**
   - Find Person 2's IP address (they're running the follower)
   - Find where Redis is running (usually Person 2's machine)
   - Edit the startup script to use these IPs

4. **Start your broker:**
   ```bash
   # Linux/Mac
   bash scripts/start_leader.sh

   # Windows
   scripts\start_leader.bat
   ```

### What You'll See

When successful, you should see:
```
Starting Leader Broker
✓ Leadership acquired! Leader at <YOUR_IP>:9092
Leader broker listening on <YOUR_IP>:9092
✓ Leader broker started successfully
Leader lease renewed (TTL: 30s)
```

### Your Code Files

**Main file:** [leader_broker/leader.py](leader_broker/leader.py:1-365)
- Line 30-51: `acquire_leadership()` - Gets leader status from Redis
- Line 53-60: `renew_lease_loop()` - Keeps renewing the lease
- Line 62-109: `handle_produce_request()` - Main logic for accepting messages
- Line 350-365: `main()` - Entry point

**Replication file:** [leader_broker/replication.py](leader_broker/replication.py:1-82)
- Line 39-80: `replicate_message()` - Sends message to follower, waits for ACK

**Log file:** [leader_broker/log_manager.py](leader_broker/log_manager.py:1-93)
- Line 29-57: `append()` - Stores messages in memory

### Key Concepts for Viva

1. **What is synchronous replication?**
   - Leader waits for follower to ACK before returning success to producer
   - See line 89-99 in leader.py

2. **How do you maintain leadership?**
   - Continuously renew lease in Redis every 5 seconds
   - See `renew_lease_loop()` at line 53-60

3. **What is High Water Mark (HWM)?**
   - Highest offset that's been replicated to follower
   - Updated at line 103 after successful replication

4. **What happens when follower doesn't ACK?**
   - Replication fails, leader doesn't update HWM
   - Producer receives error
   - See line 95-98

### Demo Day - Your Responsibilities

1. Start your leader broker first (after Redis)
2. Show logs when producer sends messages
3. Show replication to follower in logs
4. When asked to simulate failure: **Kill your process** (Ctrl+C or kill command)
5. Show that follower takes over

---

## 👤 Person 2: Follower Broker + Redis (Node 2)

### Your Role
You're responsible for:
1. **Follower Broker** - Replicates data from leader, becomes leader on failure
2. **Redis Server** - The metadata store for the entire system

### Your Machine Setup

1. **Install Redis:**
   ```bash
   # Ubuntu/Debian
   sudo apt update && sudo apt install redis-server

   # macOS
   brew install redis

   # Windows
   # Download from: https://github.com/microsoftarchive/redis/releases
   ```

2. **Start Redis:**
   ```bash
   # Linux
   sudo systemctl start redis

   # macOS
   brew services start redis

   # Manual start
   redis-server
   ```

3. **Verify Redis:**
   ```bash
   redis-cli ping
   # Should return: PONG
   ```

4. **Install Python dependencies:**
   ```bash
   cd airline-kafka-pipeline
   python3 -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

5. **Start your broker:**
   ```bash
   # Linux/Mac
   bash scripts/start_follower.sh

   # Windows
   scripts\start_follower.bat
   ```

### What You'll See

**Normal operation (while leader is alive):**
```
Starting Follower Broker
Follower broker listening on <YOUR_IP>:9093
Started monitoring leader health...
Leader alive: <LEADER_IP>:9092
✓ Replicated message at offset 0
✓ Replicated message at offset 1
```

**When leader fails:**
```
⚠ Leader lease not found (attempt 1/3)
⚠ Leader lease not found (attempt 2/3)
⚠ Leader lease not found (attempt 3/3)
Leader failure detected! Attempting election...
✓✓✓ LEADERSHIP ACQUIRED! ✓✓✓
New leader: <YOUR_IP>:9093
🎉 PROMOTION TO LEADER! 🎉
✓ Now acting as leader
```

### Your Code Files

**Main file:** [follower_broker/follower.py](follower_broker/follower.py:1-261)
- Line 39-52: `on_become_leader()` - Called when promoted to leader
- Line 61-84: `handle_replicate_request()` - Receives data from leader
- Line 86-118: `handle_produce_request()` - Rejects writes unless promoted

**Election file:** [follower_broker/election.py](follower_broker/election.py:1-83)
- Line 25-58: `monitor_leader_health()` - Checks leader heartbeat every 5 seconds
- Line 60-77: `attempt_leader_election()` - Uses Redis SETNX to become leader

### Key Concepts for Viva

1. **How do you detect leader failure?**
   - Check Redis lease every 5 seconds
   - If lease missing 3 times in a row (15 seconds), leader is dead
   - See election.py line 33-46

2. **How do you prevent split-brain?**
   - Use Redis SETNX (SET if Not eXists) - atomic operation
   - Only one follower can successfully set the key
   - See election.py line 69

3. **What do you do when you receive a write request as follower?**
   - Reject it with "NOT_THE_LEADER" error
   - See follower.py line 90-93

4. **What happens to your replicated data when you become leader?**
   - It's already there! You have all committed messages
   - Just start accepting new writes
   - See follower.py line 39-52

### Demo Day - Your Responsibilities

1. Start Redis first
2. Start follower broker second
3. Show replication logs when leader sends data
4. When Person 1 kills their leader:
   - Point to your logs showing failure detection
   - Point to logs showing election
   - Point to logs showing promotion
5. Show that producer can now write to you

---

## 👤 Person 3: Producer Client (Node 3)

### Your Role
You send messages to the broker cluster. Your client is smart - it automatically finds the leader and reconnects if it fails.

### Your Machine Setup

1. **Install dependencies:**
   ```bash
   cd airline-kafka-pipeline
   python3 -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Get IP addresses:**
   - Person 1's IP (Leader Broker)
   - Person 2's IP (Follower Broker and Redis)

3. **Start producer:**
   ```bash
   # Linux/Mac
   bash scripts/start_producer.sh

   # Windows
   scripts\start_producer.bat
   ```

### How to Use

**Interactive mode:**
```bash
Message > Hello World!
✓ Message acknowledged at offset 0

Message > This is a test
✓ Message acknowledged at offset 1

Message > batch 10
Sending 10 test messages...
✓ Batch complete: Success: 10, Failed: 0
```

**Batch mode (for demo):**
```bash
python -m producer.producer \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --batch 100
```

**Single message:**
```bash
python -m producer.producer \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --message "Important message"
```

### What You'll See

**Normal operation:**
```
Discovered leader: <LEADER_IP>:9092
✓ Connected to leader
Sent message: Hello World!
✓ Message acknowledged at offset 0
```

**During failover:**
```
Error sending message: Connection refused
⚠ Broker is not the leader - discovering new leader...
Discovered leader: <FOLLOWER_IP>:9093
✓ Connected to leader
Sent message: Hello World!
✓ Message acknowledged at offset 1
```

### Your Code Files

**Main file:** [producer/producer.py](producer/producer.py:1-270)
- Line 30-66: `discover_leader()` - Queries Redis for current leader
- Line 68-95: `connect_to_leader()` - Establishes TCP connection
- Line 97-169: `send_message()` - Main send logic with retry and failover
- Line 193-249: `interactive_mode()` - CLI interface

### Key Concepts for Viva

1. **How do you discover the leader?**
   - Query Redis for leader metadata
   - If Redis unavailable, query brokers directly
   - See line 30-66

2. **What happens when leader fails mid-send?**
   - Get connection error or "NOT_THE_LEADER" response
   - Discover new leader from Redis
   - Retry the same message (with same UUID for deduplication)
   - See line 140-155

3. **How do you prevent duplicate messages?**
   - Each message has a UUID
   - If retrying, use same UUID
   - Broker detects and ignores duplicates
   - See line 100

4. **What is the ACK you're waiting for?**
   - Confirmation that message is replicated and committed
   - Only then is offset returned
   - See line 114-123

### Demo Day - Your Responsibilities

1. Send 100 test messages before failover
2. Show successful acknowledgments
3. After leader fails:
   - Show automatic discovery of new leader
   - Send another message successfully
4. Prove failover is transparent to you

---

## 👤 Person 4: Consumer Client (Node 4)

### Your Role
You read messages from the broker cluster. Your client tracks what you've read and can resume after failures.

### Your Machine Setup

1. **Install dependencies:**
   ```bash
   cd airline-kafka-pipeline
   python3 -m venv venv
   source venv/bin/activate  # Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

2. **Get IP addresses:**
   - Person 1's IP (Leader Broker)
   - Person 2's IP (Follower Broker and Redis)

3. **Start consumer:**
   ```bash
   # Linux/Mac
   bash scripts/start_consumer.sh

   # Windows
   scripts\start_consumer.bat
   ```

### How to Use

**Interactive mode:**
```bash
Consumer > fetch
[Offset 0] Hello World!
[Offset 1] Test message
✓ Fetched 2 messages

Consumer > all
[Offset 0] Hello World!
[Offset 1] Test message
...
✓ Fetched 100 messages

Consumer > start
# Continuous consumption (Ctrl+C to stop)
```

**Fetch all messages (for demo):**
```bash
python -m consumer.consumer \
    --consumer-id "demo-consumer" \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --fetch-all
```

**Continuous consumption:**
```bash
python -m consumer.consumer \
    --consumer-id "demo-consumer" \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --continuous
```

### What You'll See

**Normal operation:**
```
Starting consumer (ID: demo-consumer)
Current offset: -1
Discovered leader: <LEADER_IP>:9092
Fetching messages from offset 0...
✓ Received 10 messages (HWM: 9)
[Offset 0] Test message 1
[Offset 1] Test message 2
...
Committed offset: 9
```

**During failover:**
```
Error fetching messages: Connection refused
⚠ Broker is not the leader - discovering new leader...
Discovered leader: <FOLLOWER_IP>:9093
✓ Connected to leader
Fetching messages from offset 10...
✓ Received 5 messages (HWM: 14)
```

### Your Code Files

**Main file:** [consumer/consumer.py](consumer/consumer.py:1-320)
- Line 31-41: `load_offset()` - Loads last read position from Redis
- Line 43-50: `commit_offset()` - Saves current position to Redis
- Line 52-88: `discover_leader()` - Finds current leader
- Line 126-180: `fetch_messages()` - Main read logic with failover
- Line 202-237: `get_all_messages()` - Fetch everything from beginning

### Key Concepts for Viva

1. **What is High Water Mark (HWM)?**
   - Highest offset that's been replicated
   - You can only read up to HWM (committed data)
   - Prevents reading uncommitted data
   - See line 168-170

2. **How do you track your position?**
   - Store offset in Redis after processing each message
   - On restart, load from Redis
   - Allows resume from where you left off
   - See line 31-50

3. **What happens when leader fails while you're reading?**
   - Get connection error
   - Discover new leader from Redis
   - Reconnect and continue from last committed offset
   - See line 157-167

4. **Why can't you read un-replicated data?**
   - If leader crashes, un-replicated data is lost
   - Only replicated data (up to HWM) is guaranteed durable
   - See line 168-170 (filter by HWM)

### Demo Day - Your Responsibilities

1. After Person 3 sends 100 messages and leader fails
2. Run fetch-all mode to get all messages
3. Count and show: 100 messages sent, 100 messages received
4. **This proves zero data loss!**
5. Show that you can read from new leader seamlessly

---

## 🎯 Demo Day Checklist

### Pre-Demo Setup (15 minutes before)

- [ ] **Person 2**: Start Redis server
- [ ] **Person 2**: Start Follower broker (should see "monitoring leader")
- [ ] **Person 1**: Start Leader broker (should see "leadership acquired")
- [ ] **Person 3**: Test producer - send 1 message
- [ ] **Person 4**: Test consumer - fetch 1 message
- [ ] **All**: Verify logs look good

### Demo Flow (5 minutes)

1. **Person 3**: Send 100 messages
   ```bash
   python -m producer.producer --batch 100 --brokers ... --redis-host ...
   ```
   - Show: "100 success, 0 failed"

2. **Person 1**: Show replication logs
   - Point out: "HWM updated to 99"

3. **Person 1**: Kill your broker
   ```bash
   # Press Ctrl+C or kill -9 <PID>
   ```

4. **Person 2**: Show failover logs (15-20 seconds)
   - Point out: "LEADERSHIP ACQUIRED"

5. **Person 3**: Send 1 more message
   ```bash
   python -m producer.producer --message "After failover" --brokers ... --redis-host ...
   ```
   - Show: Automatically connected to new leader

6. **Person 4**: Fetch all messages
   ```bash
   python -m consumer.consumer --fetch-all --brokers ... --redis-host ... | grep "Offset" | wc -l
   ```
   - Show: **100 messages** (ZERO DATA LOSS!)

### Viva Questions to Prepare

**Everyone should know:**
- Architecture diagram
- What is your component's role?
- How does failover work end-to-end?

**Person 1:**
- Synchronous replication
- HWM management
- Leader lease

**Person 2:**
- Leader election algorithm
- Redis SETNX
- Heartbeat mechanism

**Person 3:**
- Leader discovery
- Automatic failover
- Message deduplication

**Person 4:**
- Offset tracking
- HWM enforcement
- Read consistency

---

## 📞 Quick Communication Template

Share this info among team:

```
Team Member: Person 1
Role: Leader Broker
Machine IP: 192.168.x.x
Port: 9092
Status: ✅ Running
```

```
Team Member: Person 2
Role: Follower Broker + Redis
Machine IP: 192.168.x.x
Broker Port: 9093
Redis Port: 6379
Status: ✅ Running
```

```
Team Member: Person 3
Role: Producer
Using Brokers: [Person1_IP:9092, Person2_IP:9093]
Using Redis: Person2_IP:6379
Status: ✅ Ready
```

```
Team Member: Person 4
Role: Consumer
Using Brokers: [Person1_IP:9092, Person2_IP:9093]
Using Redis: Person2_IP:6379
Status: ✅ Ready
```

---

Good luck with your demo! 🚀
