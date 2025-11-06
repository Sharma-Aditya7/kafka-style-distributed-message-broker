# YAK Message Broker - Setup Guide

## Overview

This guide walks you through setting up the YAK (Yet Another Kafka) distributed message broker system across 4 physical lab machines.

## Architecture

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

## Prerequisites

### All Nodes (1-4)
- Python 3.8 or higher
- Network connectivity between all machines
- Git (for cloning the repository)

### Node 2 Only
- Redis server installed

## Installation Steps

### Step 1: Install Python Dependencies (All Nodes)

```bash
# Clone the repository
git clone <your-repo-url>
cd airline-kafka-pipeline

# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On Linux/Mac:
source venv/bin/activate
# On Windows:
venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt
```

### Step 2: Install Redis (Node 2 Only)

#### On Ubuntu/Debian:
```bash
sudo apt update
sudo apt install redis-server
sudo systemctl start redis
sudo systemctl enable redis
```

#### On macOS:
```bash
brew install redis
brew services start redis
```

#### On Windows:
Download Redis for Windows from: https://github.com/microsoftarchive/redis/releases
Or use WSL with Ubuntu instructions above.

#### Verify Redis is Running:
```bash
redis-cli ping
# Should return: PONG
```

### Step 3: Configure IP Addresses

Each team member needs to update the configuration with actual machine IPs.

#### Find Your IP Address:

**Linux/Mac:**
```bash
ip addr show  # or ifconfig
```

**Windows:**
```cmd
ipconfig
```

#### Update Configuration Files:

Copy the example config:
```bash
cp config.env.example config.env
```

Edit `config.env` with actual IP addresses:

**Person 1 (Leader - Node 1):**
```bash
LEADER_HOST=192.168.1.101      # Your IP
FOLLOWER_HOST=192.168.1.102    # Person 2's IP
REDIS_HOST=192.168.1.102       # Person 2's IP (where Redis runs)
```

**Person 2 (Follower - Node 2):**
```bash
FOLLOWER_HOST=192.168.1.102    # Your IP
REDIS_HOST=192.168.1.102       # Your IP (if running Redis locally)
```

**Person 3 (Producer - Node 3):**
```bash
LEADER_HOST=192.168.1.101      # Person 1's IP
FOLLOWER_HOST=192.168.1.102    # Person 2's IP
REDIS_HOST=192.168.1.102       # Person 2's IP
```

**Person 4 (Consumer - Node 4):**
```bash
CONSUMER_ID=consumer-1
LEADER_HOST=192.168.1.101      # Person 1's IP
FOLLOWER_HOST=192.168.1.102    # Person 2's IP
REDIS_HOST=192.168.1.102       # Person 2's IP
```

### Step 4: Configure Firewall Rules

Each broker machine needs to allow incoming connections.

#### On Ubuntu/Debian:
```bash
# Person 1 (Node 1):
sudo ufw allow 9092/tcp

# Person 2 (Node 2):
sudo ufw allow 9093/tcp
sudo ufw allow 6379/tcp  # Redis
```

#### On Windows:
Open Windows Firewall → Inbound Rules → New Rule → Port → Allow the specific ports (9092, 9093, 6379)

### Step 5: Test Network Connectivity

From any node, test connectivity to others:

```bash
# Test Leader (from any node)
telnet 192.168.1.101 9092

# Test Follower (from any node)
telnet 192.168.1.102 9093

# Test Redis (from any node)
redis-cli -h 192.168.1.102 ping
```

## Starting the System

### Order of Startup:

1. **Node 2: Start Redis** (if not already running)
   ```bash
   sudo systemctl start redis
   ```

2. **Node 1: Start Leader Broker**
   ```bash
   # Linux/Mac:
   bash scripts/start_leader.sh

   # Windows:
   scripts\start_leader.bat
   ```

   Look for: `✓ Leadership acquired!`

3. **Node 2: Start Follower Broker**
   ```bash
   # Linux/Mac:
   bash scripts/start_follower.sh

   # Windows:
   scripts\start_follower.bat
   ```

   Look for: `Started monitoring leader health...`

4. **Node 3: Start Producer (when ready to send messages)**
   ```bash
   # Linux/Mac:
   bash scripts/start_producer.sh

   # Windows:
   scripts\start_producer.bat
   ```

5. **Node 4: Start Consumer (when ready to read messages)**
   ```bash
   # Linux/Mac:
   bash scripts/start_consumer.sh

   # Windows:
   scripts\start_consumer.bat
   ```

## Verifying Setup

### Check 1: Leader Broker Logs
You should see:
```
✓ Leadership acquired! Leader at <IP>:9092
Leader broker listening on <IP>:9092
✓ Leader broker started successfully
Leader lease renewed (TTL: 30s)
```

### Check 2: Follower Broker Logs
You should see:
```
Follower broker listening on <IP>:9093
Started monitoring leader health...
Leader alive: <LEADER_IP>:9092
```

### Check 3: Send Test Message
In the Producer terminal:
```
Message > Hello YAK!
Sent message: Hello YAK!
✓ Message acknowledged at offset 0
```

### Check 4: Consume Message
In the Consumer terminal:
```
Consumer > fetch
[Offset 0] Hello YAK!
✓ Fetched 1 messages
```

## Troubleshooting

### Issue: "Failed to connect to Redis"
- **Solution:** Ensure Redis is running on Node 2
- Check: `redis-cli -h <REDIS_HOST> ping`

### Issue: "Failed to connect to follower"
- **Solution:** Ensure Follower broker is running and IP is correct
- Check firewall rules on Node 2

### Issue: "Could not acquire leadership"
- **Solution:** Another leader already exists
- Check Redis: `redis-cli -h <REDIS_HOST> GET leader:current`
- Clear if needed: `redis-cli -h <REDIS_HOST> DEL leader:lease leader:current`

### Issue: "Connection refused" errors
- **Solution:** Check firewall rules and network connectivity
- Verify IPs are correct in config
- Test with telnet: `telnet <IP> <PORT>`

### Issue: Producer can't send messages
- **Solution:** Ensure Leader broker is running
- Check Producer logs for "Discovered leader" message
- Verify LEADER_HOST and FOLLOWER_HOST in config

### Issue: Consumer can't read messages
- **Solution:** Ensure messages have been sent first
- Check HWM: Messages must be replicated to be readable
- Try: `Consumer > all` to fetch all messages from beginning

## Next Steps

Once setup is complete, proceed to [DEMO.md](DEMO.md) for running the failover demonstration.

## Team Member Responsibilities

### Person 1 (Leader Broker)
- ✅ Install Python and dependencies
- ✅ Update config with your IP
- ✅ Open port 9092 in firewall
- ✅ Start leader broker first (after Redis)

### Person 2 (Follower Broker)
- ✅ Install Python and dependencies
- ✅ Install and start Redis server
- ✅ Update config with your IP
- ✅ Open ports 9093 and 6379 in firewall
- ✅ Start follower broker second

### Person 3 (Producer)
- ✅ Install Python and dependencies
- ✅ Update config with broker IPs
- ✅ Start producer and send test messages

### Person 4 (Consumer)
- ✅ Install Python and dependencies
- ✅ Update config with broker IPs
- ✅ Start consumer and verify messages received
