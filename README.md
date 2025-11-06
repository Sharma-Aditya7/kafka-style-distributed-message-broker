# YAK - Yet Another Kafka

A fault-tolerant distributed message broker built from scratch in Python, demonstrating core distributed systems concepts.

## 🎯 Project Overview

YAK is a custom implementation of a Kafka-like message broker system with:
- **Zero Data Loss**: Synchronous replication ensures no committed message is ever lost
- **Automatic Failover**: Leader failure detected and recovered within seconds
- **High Availability**: Follower automatically promotes to leader on failure
- **Client Intelligence**: Producers and consumers automatically discover and reconnect to the new leader

## 📐 Architecture

```
┌─────────────────┐         ┌─────────────────┐
│   Node 1        │         │   Node 2        │
│  Leader Broker  │◄───────►│ Follower Broker │
│  (Port 9092)    │Replicate│  (Port 9093)    │
└────────┬────────┘         └────────┬────────┘
         │                           │
         │                  ┌────────▼────────┐
         │                  │  Redis Server   │
         │                  │  (Port 6379)    │
         │                  └─────────────────┘
         │                           │
    ┌────▼────┐                 ┌───▼─────┐
    │  Node 3 │                 │ Node 4  │
    │Producer │                 │Consumer │
    └─────────┘                 └─────────┘
```

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Redis server
- 4 networked machines (or localhost for testing)

### Installation

```bash
# Clone repository
git clone <your-repo-url>
cd airline-kafka-pipeline

# Install dependencies
pip install -r requirements.txt
```

### Start the System

1. **Start Redis** (Node 2):
   ```bash
   redis-server
   ```

2. **Start Leader Broker** (Node 1):
   ```bash
   python -m leader_broker.leader \
       --host 0.0.0.0 \
       --port 9092 \
       --follower-host <FOLLOWER_IP> \
       --follower-port 9093 \
       --redis-host <REDIS_IP>
   ```

3. **Start Follower Broker** (Node 2):
   ```bash
   python -m follower_broker.follower \
       --host 0.0.0.0 \
       --port 9093 \
       --redis-host localhost
   ```

4. **Send Messages** (Node 3):
   ```bash
   python -m producer.producer \
       --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
       --redis-host <REDIS_IP> \
       --batch 100
   ```

5. **Consume Messages** (Node 4):
   ```bash
   python -m consumer.consumer \
       --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
       --redis-host <REDIS_IP> \
       --fetch-all
   ```

## 📚 Documentation

- **[SETUP.md](SETUP.md)** - Complete setup guide for 4-node deployment
- **[DEMO.md](DEMO.md)** - Step-by-step failover demonstration
- **[config.env.example](config.env.example)** - Configuration template

## 🏗️ Project Structure

```
airline-kafka-pipeline/
├── common/                    # Shared utilities
│   ├── protocol.py           # Message protocol and serialization
│   ├── redis_client.py       # Redis metadata store wrapper
│   └── config.py             # Configuration management
├── leader_broker/            # Leader broker implementation
│   ├── leader.py             # Main leader logic
│   ├── log_manager.py        # Message log storage
│   └── replication.py        # Replication manager
├── follower_broker/          # Follower broker implementation
│   ├── follower.py           # Main follower logic
│   ├── election.py           # Leader election logic
│   └── log_manager.py        # Message log storage
├── producer/                 # Producer client
│   └── producer.py           # Producer with auto-failover
├── consumer/                 # Consumer client
│   └── consumer.py           # Consumer with offset tracking
├── scripts/                  # Startup and demo scripts
│   ├── start_leader.sh/bat   # Start leader broker
│   ├── start_follower.sh/bat # Start follower broker
│   ├── start_producer.sh/bat # Start producer client
│   ├── start_consumer.sh/bat # Start consumer client
│   ├── demo_failover.sh      # Automated failover demo
│   └── test_integration.py   # Integration tests
├── requirements.txt          # Python dependencies
├── config.env.example        # Configuration template
├── SETUP.md                  # Setup guide
├── DEMO.md                   # Demo guide
└── README.md                 # This file
```

## 👥 Team Roles

### Person 1: Leader Broker (Node 1)
**Responsibilities:**
- Accepts all write requests from producers
- Synchronously replicates to follower
- Manages leader lease in Redis
- Updates High Water Mark (HWM)

**Files to Focus On:**
- [leader_broker/leader.py](leader_broker/leader.py)
- [leader_broker/replication.py](leader_broker/replication.py)
- [leader_broker/log_manager.py](leader_broker/log_manager.py)

**Startup:**
```bash
bash scripts/start_leader.sh
# or
scripts\start_leader.bat
```

---

### Person 2: Follower Broker (Node 2)
**Responsibilities:**
- Receives replicated data from leader
- Monitors leader health via heartbeat
- Performs leader election on failure
- Promotes to leader when needed
- Hosts Redis metadata store

**Files to Focus On:**
- [follower_broker/follower.py](follower_broker/follower.py)
- [follower_broker/election.py](follower_broker/election.py)
- [follower_broker/log_manager.py](follower_broker/log_manager.py)

**Startup:**
```bash
# Start Redis first
redis-server

# Then start follower
bash scripts/start_follower.sh
# or
scripts\start_follower.bat
```

---

### Person 3: Producer Client (Node 3)
**Responsibilities:**
- Sends messages to current leader
- Discovers leader via Redis
- Automatic failover on leader failure
- Message deduplication with UUIDs

**Files to Focus On:**
- [producer/producer.py](producer/producer.py)

**Startup:**
```bash
bash scripts/start_producer.sh
# or
scripts\start_producer.bat
```

**Usage:**
```bash
# Interactive mode
Message > Hello World!

# Batch mode
Message > batch 100

# Single message
python -m producer.producer --brokers <BROKERS> --message "Test"
```

---

### Person 4: Consumer Client (Node 4)
**Responsibilities:**
- Reads messages from current leader
- Respects High Water Mark (HWM)
- Tracks and commits offsets to Redis
- Automatic failover on leader failure

**Files to Focus On:**
- [consumer/consumer.py](consumer/consumer.py)

**Startup:**
```bash
bash scripts/start_consumer.sh
# or
scripts\start_consumer.bat
```

**Usage:**
```bash
# Interactive mode
Consumer > fetch    # Fetch next batch
Consumer > all      # Fetch all messages
Consumer > start    # Continuous consumption

# Fetch all mode
python -m consumer.consumer --brokers <BROKERS> --fetch-all
```

---

## 🔑 Key Features

### 1. Synchronous Replication
- Leader waits for follower ACK before acknowledging producer
- Guarantees durability of committed messages
- No data loss on leader failure

### 2. Leader Election
- Uses Redis SETNX for atomic leader election
- Heartbeat-based failure detection (15-second timeout)
- Prevents split-brain scenarios

### 3. High Water Mark (HWM)
- Tracks highest replicated offset
- Consumers can only read committed data
- Ensures read-after-write consistency

### 4. Client Failover
- Automatic leader discovery via Redis
- Retry logic with exponential backoff
- Transparent reconnection on failure

### 5. Message Deduplication
- UUID-based message IDs
- Prevents duplicate processing on retries
- Idempotent operations

## 🧪 Testing

### Run Integration Tests
```bash
python scripts/test_integration.py
```

Tests include:
- ✅ Basic produce and consume
- ✅ Batch message production
- ✅ Consumer offset tracking
- ✅ High Water Mark enforcement

### Run Failover Demo
```bash
bash scripts/demo_failover.sh
```

Demonstrates:
1. Send 100 messages
2. Kill leader
3. Follower auto-promotes
4. Consumer reads all 100 messages (zero data loss!)

## 📊 Performance

| Metric | Value |
|--------|-------|
| Message Throughput | ~100-500 msg/sec |
| Replication Latency | ~10-50ms |
| Failover Time | ~15-20 seconds |
| Data Loss on Failure | **0%** |

## 🛠️ Configuration

Edit `config.env` to customize:

```bash
# Broker addresses
LEADER_HOST=192.168.1.101
LEADER_PORT=9092
FOLLOWER_HOST=192.168.1.102
FOLLOWER_PORT=9093

# Redis
REDIS_HOST=192.168.1.102
REDIS_PORT=6379

# Timeouts
LEADER_LEASE_TTL=30        # Lease validity (seconds)
HEARTBEAT_INTERVAL=5       # Heartbeat frequency (seconds)
HEARTBEAT_TIMEOUT=15       # Failure detection time (seconds)
```

## 🐛 Troubleshooting

### Connection Issues
```bash
# Test Redis connectivity
redis-cli -h <REDIS_IP> ping

# Test broker connectivity
telnet <BROKER_IP> 9092
```

### Clear Redis State
```bash
redis-cli -h <REDIS_IP> FLUSHALL
```

### View Logs
All components print detailed logs to stdout. Check for:
- `✓` - Success messages
- `⚠` - Warnings
- `✗` - Errors

## 📖 Learning Outcomes

By building this project, you'll learn:

1. **Distributed Consensus**: Leader election using atomic operations
2. **Replication Protocols**: Synchronous replication for durability
3. **Fault Tolerance**: Automatic failover and recovery
4. **Network Programming**: TCP socket programming in Python
5. **Client Design**: Smart clients with retry and failover logic
6. **Metadata Management**: Using Redis as a coordination service

## 🎓 Academic Context

This project is part of **Big Data 2025 (UE23CS343AB2)** course at PES University.

**Project:** YAK - Yet Another Kafka
**Team Size:** 4 members
**Evaluation:**
- Individual Component: 10 marks
- Viva: 15 marks
- End-to-End Pipeline: 5 marks

## 📝 License

This is an academic project for educational purposes.

## 🙏 Acknowledgments

Inspired by Apache Kafka's architecture and design principles.

---

## 🚀 Ready to Get Started?

1. Read [SETUP.md](SETUP.md) for installation instructions
2. Configure your 4 nodes with actual IP addresses
3. Start all components in order
4. Run the failover demo from [DEMO.md](DEMO.md)
5. Present your working system! 🎉

---

**Built with ❤️ by Team YAK**
