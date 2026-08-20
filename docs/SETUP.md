# Setup

Two ways to run YAK:

- **[Single machine](#single-machine)** — everything on localhost. Use this first; it
  exercises the full system including failover.
- **[Four machines](#four-machines)** — the original lab deployment, one node per host.

---

## Prerequisites

| | Needed for | Notes |
|---|---|---|
| Python 3.8+ | everything | |
| Redis server | everything | Only one instance, cluster-wide. |
| Java 8/11/17 | Spark job only | Required by PySpark. Skip if you're not running Spark. |

Check what you have:

```bash
python3 --version
redis-server --version
java -version
```

---

## Install

```bash
git clone https://github.com/<your-username>/<repo>.git
cd <repo>
python3 -m venv venv
source venv/bin/activate          # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

`requirements.txt` pins:

```
redis==5.0.1
pytest==7.4.3
pyspark==3.5.0
pandas==2.1.4
numpy==1.26.2
matplotlib==3.8.2
```

Only `redis` is needed for the broker itself. `pyspark`/`pandas`/`numpy`/`matplotlib`
are for the Spark analytics job; `pytest` is listed but the repository's tests are a
standalone script rather than a pytest suite (see [TESTING.md](TESTING.md)).

### Redis

```bash
# Ubuntu / Debian
sudo apt update && sudo apt install redis-server
sudo systemctl start redis

# macOS
brew install redis && brew services start redis

# Windows — use WSL, or Memurai / the Microsoft archive build
```

Verify:

```bash
redis-cli ping     # → PONG
```

---

## Configuration

Every component takes its addresses as **command-line flags**, and those flags are what
the startup scripts and this guide use. `common/config.py` additionally reads defaults
from environment variables, so `config.env` is only useful if you export it into your
shell:

```bash
cp config.env.example config.env
# edit config.env, then:
set -a; source config.env; set +a
```

If you pass explicit `--host` / `--brokers` / `--redis-host` flags — as everything below
does — you can ignore `config.env` entirely.

Key tunables, with the defaults that ship here:

| Setting | Default | Effect |
|---|---|---|
| `LEADER_LEASE_TTL` | 30s | How long the lease survives without renewal. |
| `HEARTBEAT_INTERVAL` | 5s | Lease renewal period, and follower poll period. |
| `REPLICATION_TIMEOUT` | 10s | Socket timeout on leader→follower replication. |
| `MAX_RETRIES` | 3 | Client retry attempts before giving up. |
| `RECONNECT_DELAY` | 2s | Fixed delay between client retries. |

Lowering `LEADER_LEASE_TTL` and `HEARTBEAT_INTERVAL` shortens failover; the follower
elects after 3 consecutive missed checks.

---

## Single machine

Four terminals, started in this order. **Start the follower before the leader** — the
leader connects to it on the first write.

**1 — Redis**

```bash
redis-server
```

**2 — Follower broker**

```bash
python -m follower_broker.follower \
    --host 0.0.0.0 \
    --port 9093 \
    --redis-host localhost
```

Expect: `Follower broker listening on 0.0.0.0:9093` and `Started monitoring leader health...`

**3 — Leader broker**

```bash
python -m leader_broker.leader \
    --host 0.0.0.0 \
    --port 9092 \
    --follower-host localhost \
    --follower-port 9093 \
    --redis-host localhost
```

Expect: `✓ Leadership acquired! Leader at 0.0.0.0:9092`, then `Leader lease renewed (TTL: 30s)`
every 5 seconds.

> `--follower-host` is required and has no default. The leader will not start without it.

**4 — Producer**

```bash
python -m producer.producer \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost
```

This drops into an interactive prompt:

```
Message > hello
✓ Message acknowledged at offset 0
Message > batch 50
Message > quit
```

**5 — Consumer** (fifth terminal, or reuse the producer's after `quit`)

```bash
python -m consumer.consumer \
    --consumer-id consumer-1 \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost
```

```
Consumer > fetch
[Offset 0] hello
Consumer > quit
```

### Interactive command reference

Both clients drop into a REPL when run without a mode flag.

**Producer** (`Message >`)

| Input | Effect |
|---|---|
| *any text* | Send that text as one message |
| `batch <N>` | Send N generated test messages, then report count and measured rate |
| `quit` / `exit` | Stop |

**Consumer** (`Consumer >`)

| Command | Effect |
|---|---|
| `fetch` | Fetch the next batch from the current offset, then commit |
| `all` | Fetch everything from the beginning (does not change the stored offset) |
| `start` | Continuous polling until Ctrl+C |
| `offset` | Print the current offset |
| `reset` | Reset the offset to `-1` and commit, so the next fetch replays from the start |
| `quit` / `exit` | Stop |

Non-interactive equivalents, useful for scripting:

```bash
python -m producer.producer --brokers … --redis-host … --message "one message"
python -m producer.producer --brokers … --redis-host … --batch 100

python -m consumer.consumer --brokers … --redis-host … --fetch-all
python -m consumer.consumer --brokers … --redis-host … --continuous
python -m consumer.consumer --brokers … --redis-host … --from-beginning --fetch-all
```

### Startup scripts

`scripts/` wraps the same commands with localhost defaults, overridable by environment
variable:

```bash
bash scripts/start_follower.sh
bash scripts/start_leader.sh
bash scripts/start_producer.sh
bash scripts/start_consumer.sh
```

```cmd
scripts\start_follower.bat
scripts\start_leader.bat
scripts\start_producer.bat
scripts\start_consumer.bat
```

Override addresses without editing files:

```bash
FOLLOWER_HOST=10.0.0.2 REDIS_HOST=10.0.0.2 bash scripts/start_leader.sh
BROKERS="10.0.0.1:9092 10.0.0.2:9093" REDIS_HOST=10.0.0.2 bash scripts/start_producer.sh
```

The `.bat` scripts hardcode their values in `set` lines near the top — edit those
directly on Windows.

---

## Flight data pipeline

With brokers running, stream the bundled dataset and analyze it. Full details in
[FLIGHT_DATA.md](FLIGHT_DATA.md).

```bash
# Stream 1000 records as fast as the broker accepts them
python -m producer.flight_data_producer \
    --csv data/FlightDelay2.csv \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --max-records 1000 \
    --fast

# Analyze everything the broker holds
python -m consumer.flight_data_consumer \
    --consumer-id flight-analytics \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --batch
```

---

## Spark

Needs Java on `PATH`:

```bash
sudo apt install openjdk-11-jdk     # or: brew install openjdk@11
java -version
```

Then:

```bash
python -m spark_jobs.flight_delay_streaming \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --save \
    --output output/flight_analysis
```

Runs Spark in local mode — no cluster required. Writes Parquet, JSON and summary CSV
under `output/` (gitignored).

---

## Four machines

The layout the project was built and demonstrated on:

| Node | Runs | Inbound ports |
|---|---|---|
| 1 | Leader broker | 9092 |
| 2 | Follower broker **+ Redis** | 9093, 6379 |
| 3 | Producer | — |
| 4 | Consumer | — |

Nodes 3 and 4 need outbound access to 9092, 9093 and 6379 — every client talks to Redis
directly for leader discovery and offset storage, not just to the brokers.

**Find each node's address:**

```bash
ip addr show        # Linux
ipconfig            # Windows
```

**Open the ports:**

```bash
# Node 1
sudo ufw allow 9092/tcp

# Node 2
sudo ufw allow 9093/tcp
sudo ufw allow 6379/tcp
```

**Let Redis accept remote connections** — by default it binds to loopback only. In
`/etc/redis/redis.conf` on Node 2:

```
bind 0.0.0.0
protected-mode no
```

then `sudo systemctl restart redis`. This leaves Redis unauthenticated and open to the
LAN; only do it on a trusted network, and set `requirepass` if you cannot rely on that.

**Verify connectivity** from Nodes 3 and 4 before starting anything:

```bash
redis-cli -h <NODE2_IP> ping      # → PONG
nc -vz <NODE1_IP> 9092
nc -vz <NODE2_IP> 9093
```

**Start, in order:**

```bash
# Node 2 — Redis, then follower
redis-server
python -m follower_broker.follower --host 0.0.0.0 --port 9093 --redis-host localhost

# Node 1 — leader
python -m leader_broker.leader --host 0.0.0.0 --port 9092 \
    --follower-host <NODE2_IP> --follower-port 9093 --redis-host <NODE2_IP>

# Node 3 — producer
python -m producer.producer \
    --brokers <NODE1_IP>:9092 <NODE2_IP>:9093 --redis-host <NODE2_IP>

# Node 4 — consumer
python -m consumer.consumer --consumer-id consumer-1 \
    --brokers <NODE1_IP>:9092 <NODE2_IP>:9093 --redis-host <NODE2_IP>
```

Always pass **both** broker addresses to `--brokers`. That list is the client's fallback
path when Redis discovery fails, and it is what makes failover work.

Bind brokers to `0.0.0.0` rather than a specific IP: the address passed to `--host` is
also what gets written into Redis as the leader address, and clients will try to connect
to it.

---

## Verifying a healthy cluster

```bash
redis-cli -h <REDIS_HOST> GET leader:current   # {"host":"...","port":9092}
redis-cli -h <REDIS_HOST> TTL  leader:lease    # counts down 30 → ~25, then resets
redis-cli -h <REDIS_HOST> GET  hwm:offset      # rises as messages commit
```

A `leader:lease` TTL that keeps resetting is the clearest sign the leader is alive.

---

## Troubleshooting

**`Cannot start as leader - another leader exists`**
A lease is still held — often a stale one from a previous run, since the lease outlives a
`kill -9` by up to 30s. Wait for expiry, or clear it:

```bash
redis-cli -h <REDIS_HOST> DEL leader:lease leader:current
```

**`Failed to connect to follower`**
The leader reports this on its first write, not at startup. Start the follower first and
confirm `--follower-host` matches where it's actually listening.

**Producer: `✗ Could not discover leader`**
Redis has no `leader:current`, and no broker in `--brokers` answered a `METADATA`
request. Check that the leader is running and that this host can reach Redis.

**Consumer returns 0 messages when data was definitely sent**
Either the consumer's committed offset is already past everything, or the HWM hasn't
advanced. Check both:

```bash
redis-cli -h <REDIS_HOST> GET hwm:offset
redis-cli -h <REDIS_HOST> GET consumer:offset:<consumer-id>
```

Restart the consumer with `--from-beginning`, or reset that key:

```bash
redis-cli -h <REDIS_HOST> SET consumer:offset:<consumer-id> -1
```

**Full reset between runs.** Broker logs are in-memory, so restarting the brokers clears
the data — but Redis keeps the HWM and offsets, which then point past an empty log. Clear
Redis whenever you restart brokers:

```bash
redis-cli -h <REDIS_HOST> FLUSHALL
```

**Spark: `JAVA_HOME is not set` / `Java gateway process exited`**
Install a JDK and export `JAVA_HOME`. PySpark 3.5 works with Java 8, 11 or 17.

---

Next: [DEMO.md](DEMO.md) to run the failover demonstration.
