# Flight Data Pipeline Guide

## Overview

This guide explains how to use the YAK message broker with **real airline flight delay data** for streaming analytics.

## Data Description

### Flight Delay Dataset

**File:** `data/FlightDelay2.csv`
**Records:** 18,339 flight records
**Format:** CSV with headers

**Fields:**
```
Marketing_Airline_Network - Airline code (UA, AA, DL, etc.)
Origin                    - Origin airport code (IAH, DFW, etc.)
Dest                      - Destination airport code
CRSDepTime                - Scheduled departure time (HHMM format)
DepDelayMinutes           - Departure delay in minutes
ArrDelayMinutes           - Arrival delay in minutes
CRSElapsedTime            - Scheduled flight duration (minutes)
Distance                  - Flight distance (miles)
```

**Sample Records:**
```csv
Marketing_Airline_Network,Origin,Dest,CRSDepTime,DepDelayMinutes,ArrDelayMinutes,CRSElapsedTime,Distance
UA,MAF,IAH,1710,0,0,95,429
UA,IAH,JAX,945,0,0,135,817
UA,IAH,ELP,1635,5,0,127,667
UA,ORF,IAD,1450,28,68,73,157
```

---

## Architecture with Flight Data

```
┌─────────────────────────────────────────────────────────────┐
│                    Flight Data Pipeline                      │
└─────────────────────────────────────────────────────────────┘

CSV File (18K records)
    │
    ▼
┌─────────────────┐         ┌─────────────────┐
│  Flight Data    │         │                 │
│   Producer      ├────────►│  Leader Broker  │
│  (Node 3)       │  JSON   │  (Node 1)       │
└─────────────────┘         └────────┬────────┘
                                     │
                                     │ Replicate
                                     ▼
                            ┌─────────────────┐
                            │ Follower Broker │
                            │  (Node 2)       │
                            └────────┬────────┘
                                     │
                    ┌────────────────┴────────────────┐
                    │                                  │
                    ▼                                  ▼
         ┌─────────────────┐              ┌─────────────────┐
         │  Analytics      │              │  Spark Job      │
         │  Consumer       │              │  (Big Data      │
         │  (Node 4)       │              │   Analytics)    │
         └─────────────────┘              └─────────────────┘
              │                                    │
              ▼                                    ▼
         Real-time Stats                    Distributed Processing
         - Delay rates                      - Advanced analytics
         - Top airlines                     - Machine learning
         - Busiest routes                   - Batch processing
```

---

## Components

### 1. Flight Data Producer (Person 3)

**File:** [producer/flight_data_producer.py](producer/flight_data_producer.py)

**What it does:**
- Reads CSV file line by line
- Converts each row to JSON format
- Streams to the broker (simulates real-time data)
- Tracks success/failure rates

**Message Format:**
```json
{
  "airline": "UA",
  "origin": "IAH",
  "destination": "DFW",
  "departure_time": "1213",
  "departure_delay": 0,
  "arrival_delay": 0,
  "flight_time": 86,
  "distance": 224,
  "timestamp": 1699272000.123
}
```

**Usage:**

```bash
# Stream all 18K records (fast mode)
python -m producer.flight_data_producer \
    --csv data/FlightDelay2.csv \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --fast

# Stream first 1000 records with 100ms delay (simulates real-time)
python -m producer.flight_data_producer \
    --csv data/FlightDelay2.csv \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --max-records 1000 \
    --delay 100

# Use startup script
bash scripts/start_flight_producer.sh
```

---

### 2. Flight Data Consumer (Person 4)

**File:** [consumer/flight_data_consumer.py](consumer/flight_data_consumer.py)

**What it does:**
- Fetches flight records from broker
- Parses JSON data
- Performs real-time analytics:
  - Overall delay statistics
  - Top airlines by flight count
  - Airlines with worst delays
  - Busiest routes
  - Routes with worst delays
  - Delays by time of day

**Analytics Output Example:**
```
======================================================================
FLIGHT DELAY ANALYTICS
======================================================================

📊 Overall Statistics:
  Total Flights Processed: 1,000
  Delayed Flights: 234
  On-Time Flights: 766
  Delay Rate: 23.40%
  Total Delay Minutes: 5,432
  Average Delay: 23.21 minutes

✈️  Top Airlines by Flight Count:
  UA: 856 flights (85.6%)
  DL: 98 flights (9.8%)
  AA: 46 flights (4.6%)

⏰ Airlines with Highest Average Delays:
  UA: 24.56 min avg (198 delayed flights)
  DL: 18.32 min avg (22 delayed flights)

🛫 Top Routes:
  IAH->DFW: 45 flights
  DFW->IAH: 42 flights
  IAH->AUS: 38 flights
```

**Usage:**

```bash
# Batch mode - fetch all and analyze
python -m consumer.flight_data_consumer \
    --consumer-id flight-analytics \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --batch

# Continuous mode - real-time analytics
python -m consumer.flight_data_consumer \
    --consumer-id flight-analytics \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --continuous

# Use startup script
bash scripts/start_flight_consumer.sh
```

---

### 3. Spark Analytics Job

**File:** [spark_jobs/flight_delay_streaming.py](spark_jobs/flight_delay_streaming.py)

**What it does:**
- Connects to YAK broker as a consumer
- Fetches all flight data
- Creates Spark DataFrame
- Performs distributed analytics:
  - Statistical analysis
  - Delay patterns by time of day
  - Distance vs delay correlation
  - Route performance analysis
- Saves results to disk (Parquet, JSON, CSV)

**Advanced Analytics:**
```
📊 Spark Flight Delay Analysis
================================================================================

Overall Statistics:
Total Flights: 18,339
Delayed Flights: 4,231
Delay Rate: 23.07%
Average Departure Delay: 12.34 minutes
Max Departure Delay: 248 minutes

✈️  Top 10 Airlines by Flight Count:
+-------+-------------+
|airline|flight_count |
+-------+-------------+
|UA     |15234        |
|DL     |1876         |
|AA     |1229         |
+-------+-------------+

⏰ Airlines by Average Delay:
+-------+---------+--------------+
|airline|avg_delay|delayed_flights|
+-------+---------+--------------+
|UA     |24.56    |3421           |
|DL     |18.32    |543            |
+-------+---------+--------------+

🕐 Delays by Departure Hour:
+---------------+---------+---------------+
|departure_hour |avg_delay|delayed_flights|
+---------------+---------+---------------+
|6              |8.45     |234            |
|17             |32.12    |456            |
|18             |41.23    |512            |
+---------------+---------+---------------+

📏 Distance vs Delay Analysis:
+-------------------+---------+---------------+------------+
|distance_category  |avg_delay|delayed_flights|avg_distance|
+-------------------+---------+---------------+------------+
|Short (<500 mi)    |18.23    |1876           |245.3       |
|Medium (500-1000mi)|24.56    |1543           |723.4       |
|Long (>1000 mi)    |28.91    |812            |1345.2      |
+-------------------+---------+---------------+------------+
```

**Usage:**

```bash
# Run Spark analytics and save results
python -m spark_jobs.flight_delay_streaming \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --save \
    --output output/flight_analysis

# Use startup script
bash scripts/start_spark_analytics.sh

# Results saved to:
# - output/flight_analysis/parquet/  (columnar format)
# - output/flight_analysis/json/     (JSON format)
# - output/flight_analysis/summary_csv/ (summary stats)
```

---

## End-to-End Demo Workflow

### Prerequisites

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Start Redis** (Node 2):
   ```bash
   redis-server
   ```

3. **Start Follower Broker** (Node 2):
   ```bash
   bash scripts/start_follower.sh
   ```

4. **Start Leader Broker** (Node 1):
   ```bash
   bash scripts/start_leader.sh
   ```

---

### Demo Steps

#### Phase 1: Normal Operation

**Step 1: Stream Flight Data**
```bash
# Person 3 (Producer)
python -m producer.flight_data_producer \
    --csv data/FlightDelay2.csv \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --max-records 1000 \
    --fast

# Expected output:
# ✓ Sent 1000 records (500.2 rec/sec)
# Total Records Sent: 1000
# Total Failed: 0
```

**What to observe:**
- **Leader logs:** "Sent replication request for offset N"
- **Follower logs:** "✓ Replicated message at offset N"
- **Leader logs:** "HWM updated to 999"

**Step 2: Real-Time Analytics**
```bash
# Person 4 (Consumer)
python -m consumer.flight_data_consumer \
    --consumer-id demo-analytics \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --batch

# Expected output:
# Total Flights Processed: 1,000
# Delayed Flights: ~230
# Delay Rate: ~23%
# [Analytics statistics...]
```

---

#### Phase 2: Failover Test

**Step 3: Kill Leader Broker**
```bash
# Person 1: Press Ctrl+C or kill process
kill -9 <LEADER_PID>
```

**What to observe:**
- **Follower logs** (15-20 seconds):
  ```
  ⚠ Leader lease not found (attempt 1/3)
  ⚠ Leader lease not found (attempt 2/3)
  ⚠ Leader lease not found (attempt 3/3)
  Leader failure detected! Attempting election...
  ✓✓✓ LEADERSHIP ACQUIRED! ✓✓✓
  New leader: <FOLLOWER_IP>:9093
  🎉 PROMOTION TO LEADER! 🎉
  ```

**Step 4: Send New Data**
```bash
# Person 3 (Producer) - automatic failover
python -m producer.flight_data_producer \
    --csv data/FlightDelay2.csv \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --max-records 100 \
    --fast

# Expected output:
# Discovered leader: <FOLLOWER_IP>:9093  # <- NEW LEADER!
# ✓ Connected to leader at <FOLLOWER_IP>:9093
# ✓ Sent 100 records
```

**Step 5: Verify Zero Data Loss**
```bash
# Person 4 (Consumer) - fetch all data
python -m consumer.flight_data_consumer \
    --consumer-id verification \
    --brokers <LEADER_IP>:9092 <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --batch

# Expected output:
# Total Flights Processed: 1,100  # <- 1000 + 100!
# ✅ ZERO DATA LOSS!
```

---

#### Phase 3: Advanced Analytics (Optional)

**Step 6: Spark Processing**
```bash
python -m spark_jobs.flight_delay_streaming \
    --brokers <FOLLOWER_IP>:9093 \
    --redis-host <REDIS_IP> \
    --save \
    --output output/flight_analysis

# Expected output:
# ✓ Fetched 1,100 records from broker
# [Comprehensive Spark analytics...]
# ✓ Saved as Parquet: output/flight_analysis/parquet
# ✓ Saved as JSON: output/flight_analysis/json
```

---

## Demo Script (Automated)

For convenience, use the automated demo script:

```bash
bash scripts/demo_complete_pipeline.sh
```

This script:
1. Streams 1000 flight records
2. Runs analytics
3. Prompts you to kill the leader
4. Waits for failover
5. Sends 100 more records
6. Verifies zero data loss
7. Optionally runs Spark analytics

---

## Key Insights from Flight Data

### What You'll Discover

1. **Delay Patterns:**
   - Certain times of day have more delays (evening flights)
   - Longer flights tend to have higher delays
   - Some routes are consistently delayed

2. **Airline Performance:**
   - Which airlines have best on-time performance
   - Average delay per airline
   - Delay distribution by carrier

3. **Route Analysis:**
   - Busiest routes (IAH-DFW, IAH-AUS)
   - Routes with worst delays
   - Geographic patterns

4. **Time Analysis:**
   - Morning flights: fewer delays
   - Evening flights: more delays
   - Peak delay hours: 5-7 PM

---

## Performance Metrics

### Expected Throughput

```
Component          | Throughput      | Latency
-------------------|-----------------|----------
Producer (Fast)    | 500-1000 rec/s  | 2-5ms
Producer (Stream)  | 10 rec/s        | 100ms
Replication        | Same as producer| 10-50ms
Consumer (Batch)   | 1000-2000 rec/s | 1-2ms
Spark Analytics    | 5000-10000 rec/s| Varies
```

### Complete Pipeline Timing

```
Task                          | Time
------------------------------|----------
Stream 1000 records (fast)    | 2-5 seconds
Stream 1000 records (sim)     | 100 seconds
Analytics (1000 records)      | 1-2 seconds
Spark analytics (18K records) | 10-30 seconds
Leader failover               | 15-20 seconds
```

---

## Troubleshooting

### Issue: CSV File Not Found

```
❌ Error: CSV file not found: data/FlightDelay2.csv
```

**Solution:**
```bash
# Ensure you're in the project root
cd airline-kafka-pipeline

# Check if file exists
ls -lh data/FlightDelay2.csv

# File should be ~520KB with 18,339 lines
wc -l data/FlightDelay2.csv
```

### Issue: JSON Parsing Errors

```
❌ Error parsing JSON: ...
```

**Solution:**
- Ensure producer is sending valid JSON
- Check producer logs for encoding issues
- Verify CSV has no corrupted rows

### Issue: No Analytics Output

```
Total Flights Processed: 0
```

**Solution:**
```bash
# Check if data was sent
redis-cli -h <REDIS_IP> GET hwm:offset
# Should show: "999" or higher

# Check consumer offset
redis-cli -h <REDIS_IP> GET consumer:offset:flight-analytics
# Should show: "-1" or valid offset

# Reset consumer offset if needed
redis-cli -h <REDIS_IP> SET consumer:offset:flight-analytics -1
```

### Issue: Spark Job Fails

```
❌ Error: Java not found
```

**Solution:**
```bash
# Install Java (required for Spark)
# Ubuntu/Debian:
sudo apt install openjdk-11-jdk

# macOS:
brew install openjdk@11

# Verify:
java -version
# Should show: openjdk version "11.x.x"
```

---

## Advanced Use Cases

### 1. Real-Time Delay Prediction

Build a model that predicts flight delays based on:
- Airline
- Route
- Time of day
- Distance
- Historical patterns

### 2. Route Optimization

Identify:
- Most reliable routes
- Best times to fly
- Airlines with best performance

### 3. Delay Alert System

Create alerts for:
- Routes with >30 min average delay
- Airlines with >25% delay rate
- Peak delay hours

---

## Next Steps

1. **Explore the data:**
   ```bash
   head -20 data/FlightDelay2.csv
   ```

2. **Stream a small sample:**
   ```bash
   python -m producer.flight_data_producer --max-records 100 --brokers localhost:9092 localhost:9093
   ```

3. **Run analytics:**
   ```bash
   python -m consumer.flight_data_consumer --batch --brokers localhost:9092 localhost:9093
   ```

4. **Test failover:**
   - Follow the demo workflow above
   - Kill leader after streaming data
   - Verify zero data loss

5. **Run Spark processing:**
   ```bash
   python -m spark_jobs.flight_delay_streaming --save --brokers localhost:9092 localhost:9093
   ```

---

## Summary

This flight data pipeline demonstrates:
- ✅ **Real-world data streaming** (18K flight records)
- ✅ **Fault-tolerant message broker** (zero data loss)
- ✅ **Real-time analytics** (delay patterns, routes, airlines)
- ✅ **Distributed processing** (Spark for big data analytics)
- ✅ **Automatic failover** (leader election, client reconnection)

**Perfect for demonstrating:**
- Distributed systems concepts
- Stream processing
- Big data analytics
- Fault tolerance
- Real-time decision making

Good luck with your demo! 🚀✈️
