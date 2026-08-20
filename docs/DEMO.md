# Demo

The demonstration this project was built around: kill the leader mid-operation and show
that every committed message survives, the follower takes over on its own, and the
clients find the new leader without being reconfigured.

Assumes a running cluster — see [SETUP.md](SETUP.md). Commands below use `localhost`;
substitute real addresses for a multi-machine run.

Start from a clean state so the counts are unambiguous:

```bash
redis-cli FLUSHALL
```

then restart both brokers (their logs are in memory, so a Redis flush without a broker
restart leaves the HWM pointing at data that no longer exists).

---

## Demo A — failover with test messages

The shortest path to the core result. Roughly 3 minutes.

### 1. Produce 100 messages

```bash
python -m producer.producer \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --batch 100
```

```
✓ Message acknowledged at offset 0
...
✓ Message acknowledged at offset 99

Results: 100 success, 0 failed
```

**Leader terminal** shows the full commit path per message:

```
Sent replication request for offset 99
Received ACK for offset 99
HWM updated to 99
✓ Message committed at offset 99
```

**Follower terminal**:

```
✓ Replicated message at offset 99
```

Confirm the watermark:

```bash
redis-cli GET hwm:offset     # → "99"
```

### 2. Kill the leader

Ctrl+C in the leader terminal, or:

```bash
# Linux / macOS
pkill -9 -f leader_broker.leader

# Windows
taskkill /F /IM python.exe
```

> A hard `kill -9` is the more interesting case: the leader never releases its lease, so
> the follower must detect expiry rather than being told. Ctrl+C calls `stop()`, which
> deletes the lease immediately and makes the promotion near-instant.

### 3. Watch the follower promote itself

Within ~15–20 seconds of the lease expiring, the **follower terminal** prints:

```
⚠ Leader lease not found (attempt 1/3)
⚠ Leader lease not found (attempt 2/3)
⚠ Leader lease not found (attempt 3/3)
Leader failure detected! Attempting election...
Attempting to acquire leadership...
✓✓✓ LEADERSHIP ACQUIRED! ✓✓✓
New leader: 0.0.0.0:9093
🎉 PROMOTION TO LEADER! 🎉
✓ Now acting as leader - accepting producer requests
```

Redis agrees:

```bash
redis-cli GET leader:current     # → {"host": "0.0.0.0", "port": 9093}
```

### 4. Produce to the new leader — same command as before

```bash
python -m producer.producer \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --message "after failover"
```

```
Discovered leader: 0.0.0.0:9093
✓ Connected to leader at 0.0.0.0:9093
✓ Message acknowledged at offset 100
```

Nothing was reconfigured. The producer read the new address out of Redis. If it still
has a cached connection to the dead leader it prints `Connection refused` or
`⚠ Broker is not the leader` first, then rediscovers.

### 5. Verify nothing was lost

```bash
python -m consumer.consumer \
    --consumer-id demo-consumer \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --from-beginning \
    --fetch-all
```

Count them:

```bash
python -m consumer.consumer \
    --consumer-id demo-consumer \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --from-beginning --fetch-all | grep -c "^\[Offset"
```

Expect **101** — the 100 committed before the crash plus the one written afterwards.
All 100 pre-crash messages are being served from the follower's replica; the original
leader's log died with its process.

`--from-beginning` matters. Without it the consumer resumes from its stored offset in
Redis and legitimately returns nothing.

---

## Demo B — failover with flight data

Same failure, real data, with analytics on both sides of it. Roughly 6 minutes.

### 1. Stream 1000 flight records

```bash
python -m producer.flight_data_producer \
    --csv data/FlightDelay2.csv \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --max-records 1000 \
    --fast
```

The producer reports its own rate as it goes and prints a summary:

```
Total Records Sent: 1000
Total Failed: 0
Total Time: <measured>
Average Rate: <measured> records/sec
```

Drop `--fast` for a `--delay 100` paced stream (~10 records/sec) if you want the
replication log to be readable in real time during a live demo.

### 2. Analyze what's there

```bash
python -m consumer.flight_data_consumer \
    --consumer-id demo-analytics \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --batch
```

Prints delay rate, top carriers, busiest routes, worst-delay carriers and routes, over
the 1000 records. Note the flight count — you'll compare against it after the failover.

### 3. Kill the leader, wait for promotion

As in Demo A, steps 2–3.

### 4. Stream 100 more into the new leader

```bash
python -m producer.flight_data_producer \
    --csv data/FlightDelay2.csv \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --max-records 100 \
    --fast
```

Watch for `Discovered leader: …:9093` in the output.

> These 100 records are re-read from the top of the same CSV, so they duplicate the
> first 100 by *content*. They are distinct messages with distinct UUIDs and distinct
> offsets, which is what the count check below cares about.

### 5. Verify the total

```bash
python -m consumer.flight_data_consumer \
    --consumer-id verification-consumer \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --batch
```

`Total Flights Processed` should read **1,100**. Use a consumer id you haven't used
before, or reset the offset first — the flight consumer has no `--from-beginning` flag
and will otherwise resume mid-stream:

```bash
redis-cli SET consumer:offset:verification-consumer -1
```

### 6. Spark (optional)

```bash
python -m spark_jobs.flight_delay_streaming \
    --brokers localhost:9092 localhost:9093 \
    --redis-host localhost \
    --save
```

Pulls the same 1,100 records through the consumer client into a Spark DataFrame and runs
the fuller analysis — hourly delay profile, distance-bucket comparison, per-route
aggregates — writing Parquet/JSON/CSV to `output/`.

---

## Scripted demos

`scripts/demo_failover.sh` and `scripts/demo_complete_pipeline.sh` automate Demos A and B
respectively. Both are **bash-only** and both **pause for you to kill the leader by
hand** — `demo_failover.sh` looks for a PID file at `/tmp/yak_leader.pid` that nothing in
this repository creates, so it falls through to a manual prompt.

```bash
bash scripts/demo_failover.sh
bash scripts/demo_complete_pipeline.sh
```

Both default to `localhost:9092 localhost:9093`; edit the variables at the top for a
multi-machine run. On Windows, run them under WSL or follow the manual steps above.

Note that `demo_failover.sh` counts messages with `wc -l` over the consumer's entire
stdout, which includes log lines — its reported count will not match 100 exactly. The
`grep -c "^\[Offset"` form in Demo A is the accurate check.

---

## Other scenarios worth showing

**Consumer resumes from its committed offset.** Consume some messages, Ctrl+C, restart
the consumer with the same `--consumer-id`. It prints `Loaded offset: N` and continues
from there rather than replaying. Kill the leader in between and it still resumes
correctly, because offsets live in Redis rather than on the broker.

**The follower refuses writes while it is a follower.** Point a producer at port 9093
before any failover:

```bash
python -m producer.producer --brokers localhost:9093 --redis-host localhost \
    --message "should be rejected"
```

The follower logs `⚠ Rejected PRODUCE request - not the leader` and the producer
rediscovers the real leader from Redis instead.

**Split-brain is prevented, not merely unlikely.** With the lease held, run a second
leader:

```bash
python -m leader_broker.leader --host 0.0.0.0 --port 9094 \
    --follower-host localhost --follower-port 9093 --redis-host localhost
```

It prints `✗ Failed to acquire leadership (another leader exists)` and exits. The
`SET NX` either succeeds or it doesn't; there is no window where both win.

---

## What this demo does *not* show

Worth saying out loud when presenting, because it's the honest boundary of the result:

- **The promoted follower runs unreplicated.** It has no follower of its own and does not
  replicate the writes it accepts after promotion. Those messages exist on one node, in
  memory. The cluster survived one failure; it would not survive a second.
- **A killed broker cannot rejoin.** Restarting the old leader prints
  `Cannot start as leader` and exits. There is no catch-up or demotion path.
- **Nothing survives a full stop.** Both logs are in memory. Stop both brokers and the
  data is gone regardless of the HWM.
- **Redis is a single point of failure.** Stop Redis and no election, discovery, or
  offset commit is possible.

The claim the demo actually supports: *committed messages survive the loss of the leader
process, and clients recover without reconfiguration.*
