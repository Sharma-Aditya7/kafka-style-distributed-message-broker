# YAK - Quick Start Guide

## 🚀 Get Running in 5 Minutes

### Step 1: Install Dependencies (All 4 machines)

```bash
cd airline-kafka-pipeline
python3 -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

---

### Step 2: Install Redis (Person 2's machine only)

```bash
# Ubuntu/Debian
sudo apt install redis-server
sudo systemctl start redis

# macOS
brew install redis
brew services start redis

# Test it
redis-cli ping  # Should return: PONG
```

---

### Step 3: Update IP Addresses (All machines)

**Find your IPs:**
```bash
# Linux/Mac
ip addr show

# Windows
ipconfig
```

**Update the startup scripts with actual IPs:**

Edit `scripts/start_leader.sh` (or `.bat` for Windows):
```bash
LEADER_HOST=0.0.0.0              # Your IP to listen on
FOLLOWER_HOST=192.168.x.x        # Person 2's IP
REDIS_HOST=192.168.x.x           # Where Redis runs (usually Person 2)
```

Edit `scripts/start_follower.sh`:
```bash
FOLLOWER_HOST=0.0.0.0            # Your IP
REDIS_HOST=localhost             # If Redis is on same machine
```

Edit `scripts/start_producer.sh`:
```bash
BROKERS="192.168.x.x:9092 192.168.x.x:9093"  # Both broker IPs
REDIS_HOST=192.168.x.x           # Redis IP
```

Edit `scripts/start_consumer.sh`:
```bash
BROKERS="192.168.x.x:9092 192.168.x.x:9093"  # Both broker IPs
REDIS_HOST=192.168.x.x           # Redis IP
```

---

### Step 4: Start Everything (In Order!)

**Person 2: Start Redis**
```bash
redis-server
```

**Person 2: Start Follower Broker**
```bash
bash scripts/start_follower.sh  # Linux/Mac
# OR
scripts\start_follower.bat      # Windows
```

**Person 1: Start Leader Broker**
```bash
bash scripts/start_leader.sh    # Linux/Mac
# OR
scripts\start_leader.bat        # Windows
```

**Person 3: Send Test Message**
```bash
bash scripts/start_producer.sh  # Linux/Mac
# OR
scripts\start_producer.bat      # Windows

# Then type:
Message > Hello YAK!
```

**Person 4: Read Messages**
```bash
bash scripts/start_consumer.sh  # Linux/Mac
# OR
scripts\start_consumer.bat      # Windows

# Then type:
Consumer > fetch
```

---

## ✅ Verify It's Working

You should see:

**Person 1 (Leader):**
```
✓ Leadership acquired! Leader at <IP>:9092
Leader lease renewed (TTL: 30s)
```

**Person 2 (Follower):**
```
Started monitoring leader health...
Leader alive: <IP>:9092
✓ Replicated message at offset 0
```

**Person 3 (Producer):**
```
Discovered leader: <IP>:9092
✓ Message acknowledged at offset 0
```

**Person 4 (Consumer):**
```
[Offset 0] Hello YAK!
```

---

## 🎬 Run the Demo

### Send 100 Messages

**Person 3:**
```bash
python -m producer.producer \
    --brokers 192.168.x.x:9092 192.168.x.x:9093 \
    --redis-host 192.168.x.x \
    --batch 100
```

### Kill the Leader

**Person 1:**
```bash
# Press Ctrl+C
# OR
kill -9 <PID>
```

### Watch Failover

**Person 2** (should see in 15-20 seconds):
```
✓✓✓ LEADERSHIP ACQUIRED! ✓✓✓
🎉 PROMOTION TO LEADER! 🎉
```

### Verify Zero Data Loss

**Person 4:**
```bash
python -m consumer.consumer \
    --brokers 192.168.x.x:9092 192.168.x.x:9093 \
    --redis-host 192.168.x.x \
    --fetch-all | grep "Offset" | wc -l
```

Should return: **100** ✅

---

## 🐛 Common Issues

### "Connection refused"
- Check if broker is running: `telnet <IP> 9092`
- Check firewall: Allow ports 9092, 9093, 6379
- Verify IP addresses in scripts

### "Failed to connect to Redis"
- Check Redis is running: `redis-cli ping`
- Verify REDIS_HOST is correct
- Check Redis port (default: 6379)

### "Cannot acquire leadership"
- Another leader already exists
- Clear Redis: `redis-cli FLUSHALL`
- Restart brokers

### Producer/Consumer can't find leader
- Check Redis has leader info: `redis-cli GET leader:current`
- Verify broker IPs in --brokers argument
- Ensure Leader broker started successfully

---

## 📁 Project Files

```
airline-kafka-pipeline/
├── common/               # Shared code (all use this)
├── leader_broker/        # Person 1's code
├── follower_broker/      # Person 2's code
├── producer/             # Person 3's code
├── consumer/             # Person 4's code
├── scripts/              # Startup scripts
├── SETUP.md             # Detailed setup
├── DEMO.md              # Demo script
├── TEAM_GUIDE.md        # Individual guides
└── README.md            # Overview
```

---

## 🎯 Quick Command Reference

### Producer Commands
```bash
# Interactive mode
Message > Your message here
Message > batch 50
Message > quit

# Single message
python -m producer.producer --message "Test" --brokers ...

# Batch mode
python -m producer.producer --batch 100 --brokers ...
```

### Consumer Commands
```bash
# Interactive mode
Consumer > fetch      # Fetch next batch
Consumer > all        # Fetch all from beginning
Consumer > start      # Continuous mode
Consumer > offset     # Show current offset
Consumer > reset      # Reset to beginning

# Fetch all mode
python -m consumer.consumer --fetch-all --brokers ...

# Continuous mode
python -m consumer.consumer --continuous --brokers ...
```

---

## 📊 Expected Performance

| Metric | Value |
|--------|-------|
| Throughput | 100-500 msg/sec |
| Replication Latency | 10-50ms |
| Failover Time | 15-20 seconds |
| Data Loss | 0% |

---

## 🎓 For Your Viva

**Be ready to explain:**
1. Your component's role
2. How failover works
3. Why zero data loss is guaranteed
4. The difference between HWM and latest offset
5. How leader election works (Redis SETNX)

**Key terms:**
- **Synchronous Replication**: Leader waits for follower ACK
- **HWM (High Water Mark)**: Highest replicated offset
- **Leader Lease**: Time-limited leadership in Redis
- **Atomic Election**: Only one follower can become leader (SETNX)
- **Client Failover**: Automatic reconnection to new leader

---

## ✨ Success Checklist

- [ ] All 4 machines can ping each other
- [ ] Redis is running and accessible
- [ ] Leader broker starts and acquires leadership
- [ ] Follower broker starts and monitors leader
- [ ] Producer can send messages
- [ ] Consumer can read messages
- [ ] Follower detects leader failure (after kill)
- [ ] Follower promotes to leader
- [ ] Producer reconnects to new leader
- [ ] Consumer reads all messages (zero data loss!)

---

**Need more help?** Read the detailed guides:
- [SETUP.md](SETUP.md) - Full setup instructions
- [DEMO.md](DEMO.md) - Demo walkthrough
- [TEAM_GUIDE.md](TEAM_GUIDE.md) - Individual team member guides
