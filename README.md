# YAK - Yet Another Kafka

A fault-tolerant distributed message broker built from scratch in Python for **real-time airline flight delay analytics**.

## 🎯 Project Overview

YAK is a custom implementation of a Kafka-like message broker system that processes **18,000+ flight delay records** with:
- **Zero Data Loss**: Synchronous replication ensures no committed message is ever lost
- **Automatic Failover**: Leader failure detected and recovered within seconds
- **Real-Time Analytics**: Live flight delay analysis and insights
- **Spark Integration**: Distributed big data processing
- **Client Intelligence**: Producers and consumers automatically discover and reconnect to the new leader

## ✈️ Flight Data Processing

This system streams and analyzes **real airline flight delay data** with:
- **18,339 flight records** from major US carriers
- **Real-time delay analytics** (routes, airlines, time patterns)
- **Spark-based distributed processing**
- **Zero data loss guarantees** during broker failures

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

4. **Stream Flight Data** (Node 3):
   ```bash
   # Stream 1000 flight records
   python -m producer.flight_data_producer \
       --csv data/FlightDelay2.csv \
       --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
       --redis-host <REDIS_IP> \
       --max-records 1000 \
       --fast
   ```

5. **Run Analytics** (Node 4):
   ```bash
   # Real-time flight delay analytics
   python -m consumer.flight_data_consumer \
       --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
       --redis-host <REDIS_IP> \
       --batch
   ```

6. **Spark Processing** (Optional):
   ```bash
   # Advanced Spark analytics
   python -m spark_jobs.flight_delay_streaming \
       --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
       --redis-host <REDIS_IP> \
       --save
   ```

## 📚 Documentation

- **[FLIGHT_DATA_GUIDE.md](FLIGHT_DATA_GUIDE.md)** - 🆕 Complete guide for flight data processing
- **[SETUP.md](SETUP.md)** - Complete setup guide for 4-node deployment
- **[DEMO.md](DEMO.md)** - Step-by-step failover demonstration
- **[TEAM_GUIDE.md](TEAM_GUIDE.md)** - Individual team member guides
- **[QUICK_START.md](QUICK_START.md)** - 5-minute quick start
- **[ARCHITECTURE.md](ARCHITECTURE.md)** - Detailed architecture documentation
- **[config.env.example](config.env.example)** - Configuration template

## 🏗️ Project Structure

```
airline-kafka-pipeline/
├── data/                           # Flight data
│   └── FlightDelay2.csv           # 🆕 18,339 flight records
├── common/                         # Shared utilities
│   ├── protocol.py                # Message protocol
│   ├── redis_client.py            # Redis wrapper
│   └── config.py                  # Configuration
├── leader_broker/                 # Leader broker (Person 1)
│   ├── leader.py
│   ├── log_manager.py
│   └── replication.py
├── follower_broker/               # Follower broker (Person 2)
│   ├── follower.py
│   ├── election.py
│   └── log_manager.py
├── producer/                      # Producer client (Person 3)
│   ├── producer.py                # Basic producer
│   └── flight_data_producer.py   # 🆕 Flight data streamer
├── consumer/                      # Consumer client (Person 4)
│   ├── consumer.py                # Basic consumer
│   └── flight_data_consumer.py   # 🆕 Flight analytics
├── spark_jobs/                    # 🆕 Spark processing
│   └── flight_delay_streaming.py # Spark analytics
├── scripts/                       # Startup scripts
│   ├── start_leader.sh/bat
│   ├── start_follower.sh/bat
│   ├── start_flight_producer.sh/bat    # 🆕 Flight producer
│   ├── start_flight_consumer.sh/bat    # 🆕 Flight consumer
│   ├── start_spark_analytics.sh/bat    # 🆕 Spark job
│   ├── demo_complete_pipeline.sh       # 🆕 Full demo
│   └── test_integration.py
├── output/                        # 🆕 Spark output
│   └── flight_analysis/           # Analytics results
├── requirements.txt               # Dependencies (+ Spark)
├── config.env.example
├── FLIGHT_DATA_GUIDE.md          # 🆕 Flight data guide
├── SETUP.md
├── DEMO.md
└── README.md
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
- Streams flight delay data from CSV
- Sends messages to current leader
- Discovers leader via Redis
- Automatic failover on leader failure
- Message deduplication with UUIDs

**Files to Focus On:**
- [producer/flight_data_producer.py](producer/flight_data_producer.py) - 🆕 Flight data streamer
- [producer/producer.py](producer/producer.py) - Basic producer

**Startup:**
```bash
# Flight data streaming
bash scripts/start_flight_producer.sh
# or
scripts\start_flight_producer.bat

# Basic producer
bash scripts/start_producer.sh
```

**Usage:**
```bash
# Stream flight data (1000 records, fast mode)
python -m producer.flight_data_producer \
    --csv data/FlightDelay2.csv \
    --brokers <BROKERS> \
    --max-records 1000 \
    --fast

# Stream all 18K records with delay (simulates real-time)
python -m producer.flight_data_producer \
    --csv data/FlightDelay2.csv \
    --brokers <BROKERS> \
    --delay 100
```

---

### Person 4: Consumer Client (Node 4)
**Responsibilities:**
- Reads flight data from current leader
- Performs real-time analytics on flight delays
- Respects High Water Mark (HWM)
- Tracks and commits offsets to Redis
- Automatic failover on leader failure

**Files to Focus On:**
- [consumer/flight_data_consumer.py](consumer/flight_data_consumer.py) - 🆕 Flight analytics
- [consumer/consumer.py](consumer/consumer.py) - Basic consumer

**Startup:**
```bash
# Flight data analytics
bash scripts/start_flight_consumer.sh
# or
scripts\start_flight_consumer.bat

# Basic consumer
bash scripts/start_consumer.sh
```

**Usage:**
```bash
# Batch analytics - fetch all and analyze
python -m consumer.flight_data_consumer \
    --brokers <BROKERS> \
    --batch

# Continuous mode - real-time analytics
python -m consumer.flight_data_consumer \
    --brokers <BROKERS> \
    --continuous
```

**Analytics Output:**
- Overall delay statistics
- Top airlines by flight count
- Airlines with worst delays
- Busiest routes
- Routes with highest delays
- Delay patterns by time of day

---

## 🆕 Spark Analytics (Bonus Component)

**File:** [spark_jobs/flight_delay_streaming.py](spark_jobs/flight_delay_streaming.py)

**What it does:**
- Connects to YAK broker as a consumer
- Fetches all flight data into Spark DataFrame
- Performs distributed big data analytics
- Saves results to disk (Parquet, JSON, CSV)

**Advanced Analytics:**
- Statistical analysis (mean, max, percentiles)
- Delay patterns by time of day
- Distance vs delay correlation
- Route performance analysis
- Airline comparison

**Usage:**
```bash
# Run Spark analytics with output
python -m spark_jobs.flight_delay_streaming \
    --brokers <BROKERS> \
    --save \
    --output output/flight_analysis

# Use startup script
bash scripts/start_spark_analytics.sh
```

**Output Files:**
- `output/flight_analysis/parquet/` - Columnar format for further processing
- `output/flight_analysis/json/` - Human-readable JSON
- `output/flight_analysis/summary_csv/` - Summary statistics

---

## 🔑 Key Features

### 1. Real Flight Data Processing
- 18,339 authentic airline flight records
- Departure and arrival delay data
- Multiple airlines, routes, and airports
- Distance and flight time information

### 2. Synchronous Replication
- Leader waits for follower ACK before acknowledging producer
- Guarantees durability of committed messages
- No data loss on leader failure

### 3. Leader Election
- Uses Redis SETNX for atomic leader election
- Heartbeat-based failure detection (15-second timeout)
- Prevents split-brain scenarios

### 4. High Water Mark (HWM)
- Tracks highest replicated offset
- Consumers can only read committed data
- Ensures read-after-write consistency

### 5. Client Failover
- Automatic leader discovery via Redis
- Retry logic with exponential backoff
- Transparent reconnection on failure

### 6. Real-Time Analytics
- Live delay statistics
- Route and airline analysis
- Time-based patterns
- Performance metrics

### 7. Spark Integration
- Distributed big data processing
- Advanced analytics
- Multiple output formats
- Scalable processing

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

### Run Flight Data Demo
```bash
bash scripts/demo_complete_pipeline.sh
```

Demonstrates:
1. Stream 1000 flight records to leader
2. Perform real-time analytics
3. Kill leader broker
4. Follower auto-promotes to leader
5. Send 100 more flight records to new leader
6. Verify zero data loss (all 1100 records present)
7. Run Spark analytics (optional)

### Run Basic Failover Demo
```bash
bash scripts/demo_failover.sh
```

Basic demo:
1. Send 100 test messages
2. Kill leader
3. Follower auto-promotes
4. Consumer reads all 100 messages (zero data loss!)

## 📊 Performance

| Metric | Value |
|--------|-------|
| Flight Record Streaming (Fast) | ~500-1000 rec/sec |
| Flight Record Streaming (Real-time) | ~10 rec/sec |
| Replication Latency | ~10-50ms |
| Failover Time | ~15-20 seconds |
| Data Loss on Failure | **0%** |
| Spark Processing (18K records) | ~10-30 seconds |
| Consumer Analytics (1K records) | ~1-2 seconds |

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

### For Flight Data Pipeline:
1. Read [FLIGHT_DATA_GUIDE.md](FLIGHT_DATA_GUIDE.md) for complete flight data guide
2. Install dependencies: `pip install -r requirements.txt`
3. Start brokers (Leader + Follower)
4. Stream flight data: `bash scripts/start_flight_producer.sh`
5. Run analytics: `bash scripts/start_flight_consumer.sh`
6. Run Spark processing: `bash scripts/start_spark_analytics.sh`
7. Test failover: `bash scripts/demo_complete_pipeline.sh`

### For Basic Setup:
1. Read [SETUP.md](SETUP.md) for installation instructions
2. Read [QUICK_START.md](QUICK_START.md) for 5-minute guide
3. Configure your 4 nodes with actual IP addresses
4. Start all components in order
5. Run the demo from [DEMO.md](DEMO.md)

---

## 📈 What Makes This Project Special?

1. **Real-World Data** - Not toy examples, actual 18K flight delay records
2. **Production Concepts** - Implements real Kafka-like distributed systems patterns
3. **Zero Data Loss** - Properly implemented synchronous replication
4. **Automatic Failover** - Self-healing system with leader election
5. **Analytics Pipeline** - Complete end-to-end data processing
6. **Spark Integration** - Big data processing capabilities
7. **Fault Tolerant** - Survives catastrophic failures
8. **Well Documented** - Comprehensive guides and examples

---

## 🎓 What You'll Learn

- **Distributed Systems**: Leader election, consensus, replication
- **Stream Processing**: Real-time data pipelines
- **Big Data**: Spark analytics on large datasets
- **Fault Tolerance**: Designing for failure
- **Network Programming**: TCP/IP, sockets, protocols
- **Data Analytics**: Flight delay patterns and insights

---

## 📊 Sample Analytics Output

```
======================================================================
FLIGHT DELAY ANALYTICS
======================================================================

📊 Overall Statistics:
  Total Flights Processed: 18,339
  Delayed Flights: 4,231
  On-Time Flights: 14,108
  Delay Rate: 23.07%
  Total Delay Minutes: 103,456
  Average Delay: 24.46 minutes

✈️  Top Airlines by Flight Count:
  UA: 15,234 flights (83.1%)
  DL: 1,876 flights (10.2%)
  AA: 1,229 flights (6.7%)

⏰ Airlines with Highest Average Delays:
  UA: 24.56 min avg (3,421 delayed flights)
  DL: 18.32 min avg (543 delayed flights)
  AA: 22.14 min avg (267 delayed flights)

🛫 Top Routes:
  IAH->DFW: 423 flights
  DFW->IAH: 398 flights
  IAH->AUS: 356 flights

🛣️  Routes with Highest Average Delays:
  ORF->IAD: 48.00 min avg
  IAD->ORF: 28.00 min avg
  IAH->MSY: 18.50 min avg
```

---

**Built with ❤️ by Team YAK**

**Perfect for:**
- Big Data course projects ✅
- Distributed systems learning ✅
- Real-world data analytics ✅
- System design interviews ✅
- Production-grade portfolio projects ✅
