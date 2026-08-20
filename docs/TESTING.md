# Testing

What this repository actually tests, how to run it, and what it doesn't cover.

The tests are **integration tests against a live cluster** — there are no unit tests and
no mocks. Redis, both brokers, and the network all have to be up. Everything runs
through `scripts/test_integration.py`.

> `pytest` is pinned in `requirements.txt`, but `test_integration.py` is a plain script
> with a `main()`, not a pytest suite. Collecting it with `pytest` will not run these
> tests. Run it directly.

---

## Running

Bring up Redis, the follower, and the leader ([SETUP.md](SETUP.md)), then:

```bash
redis-cli FLUSHALL        # start clean — see caveat below
```

Restart both brokers after the flush, then:

```bash
python scripts/test_integration.py
```

The script prints a checklist and waits on `input()` for you to press Enter, so it
cannot run unattended or in CI as written.

### Addressing

`test_integration.py` takes no arguments. It builds its broker list from
`common/config.py`, which reads environment variables and falls back to
`LEADER_HOST=0.0.0.0`, `FOLLOWER_HOST=0.0.0.0`, `REDIS_HOST=localhost`. For anything
other than a same-host run, export the real values first:

```bash
export LEADER_HOST=<node1-ip>
export FOLLOWER_HOST=<node2-ip>
export REDIS_HOST=<node2-ip>
python scripts/test_integration.py
```

In practice the broker addresses matter less than they look: both clients discover the
leader through Redis first and only fall back to the `--brokers` list. `REDIS_HOST` is
the one that must be right.

### Start clean

Test 1 asserts an **exact** message count. It resets its consumer's offset to `-1` and
expects to read back exactly the 3 messages it just sent — so it fails if the broker log
already holds messages from an earlier run. Flush Redis *and* restart both brokers
before a run. Restarting the brokers is what actually empties the log; the logs are in
memory, and flushing Redis alone just leaves the HWM pointing at data that no longer
exists.

---

## The tests

### 1 — Basic produce and consume

**Tests:** the end-to-end path. A message accepted by the leader is replicated, admitted
below the HWM, returned to a consumer, and comes back byte-identical and in order.

**How:** sends 3 messages via `Producer`, waits 2s, fetches with `Consumer`, then asserts
the count matches and each payload equals what was sent at the same index.

**Expected:** `✓✓✓ TEST 1 PASSED ✓✓✓`. Fails on a count mismatch or any payload
mismatch.

**Caveat:** the index-wise comparison assumes the fetch starts at offset 0, which is why
a clean cluster is required.

---

### 2 — Batch produce

**Tests:** that sustained sequential production works and every message is acknowledged
— i.e. synchronous replication holds up over 50 consecutive round trips without a
timeout or dropped ACK.

**How:** `producer.send_batch()` with 50 messages, timing the whole batch.

**Expected:** `Failed: 0`. Any failure fails the test.

**On the throughput line:** the test prints `Throughput: N msg/s`. That is a measurement
of *your* run — one process, one connection, one synchronous replication round trip per
message — not a recorded benchmark. No throughput figure from any past run is stored in
this repository, and none is claimed anywhere in these docs.

---

### 3 — Consumer offset tracking

**Tests:** that committed offsets are durable in Redis and survive the consumer process,
which is what lets a consumer resume rather than replay after a crash or a failover.

**How:** consumer A resets to `-1`, fetches up to 5 messages, commits the last offset,
and closes. A second consumer object is constructed with the *same* `consumer-id`; the
test asserts it loads that exact offset from Redis at construction.

**Expected:** `✓ Offset correctly persisted: N`.

**Caveat:** if no messages are available the test prints a skip notice and **returns
`True`** — it reports as passed without having tested anything. Only trust this one when
tests 1 and 2 produced data first.

---

### 4 — High water mark

**Tests, as written:** that a message which has been replicated becomes readable — the
positive half of the HWM rule.

**How:** sends one message, waits 1s, resets the consumer to `-1`, fetches, asserts at
least one message came back.

**Expected:** `✓ Consumer can read replicated messages`.

**Caveat — read this one carefully.** Despite the name, this test does **not** verify
HWM *enforcement*. Enforcement is the negative case: a message present in the leader's
log but not yet replicated must be invisible to consumers. Proving that requires
observing the window between the local append and the follower's ACK, which the test
never does. And like test 3, it returns `True` on the "no messages available" path, so
it can pass without asserting anything.

The enforcement logic itself is a one-line filter in both brokers:

```python
filtered_messages = [msg for msg in messages if msg['offset'] <= hwm]
```

That is real, and reviewable — but it is not covered by an automated test here.

---

## Failover: manually verified

The system's headline behaviour has **no automated test**. It was exercised by hand and
by the demo scripts, following [DEMO.md](DEMO.md):

| Scenario | How it was exercised | Observable result |
|---|---|---|
| Leader process killed | Ctrl+C / `kill -9` on the leader | Follower logs 3 missed lease checks, then `LEADERSHIP ACQUIRED` |
| Follower promotion | Redis lease expiry | `leader:current` flips to the follower's address |
| Producer failover | Produce after the kill, unchanged command | `Discovered leader: …:9093`, message ACKed |
| Consumer failover | Fetch-all after the kill | All pre-crash committed messages returned |
| Committed-message survival | Count consumed vs. produced | 100 sent → 100 read back after leader loss |
| Follower rejects writes | Produce directly to :9093 pre-failover | `NOT_THE_LEADER` error, client rediscovers |
| Split-brain prevented | Start a second leader while a lease is held | `Failed to acquire leadership`, process exits |

These are reproducible from the documented commands, and the log lines above are what
the code prints on those paths. They are not, however, captured as assertions anywhere,
and this repository stores no recorded run output. Treat them as "reproduce it yourself"
rather than as certified results.

---

## Not covered

Honest gaps, roughly in order of how much they matter:

- **Failover has no automated test.** The most important property in the system is
  verified only by hand.
- **HWM enforcement (the negative case) is untested** — see test 4.
- **Concurrent producers are untested.** The known ordering caveat in
  [ARCHITECTURE.md](ARCHITECTURE.md#ordering-caveat) — interleaved append/replicate
  across threads — would show up here if it were tested. It isn't.
- **Network partitions are untested.** Only clean process death was exercised, never a
  leader that is alive but unreachable, or one partitioned from Redis while still
  serving clients. The lease model makes the latter interesting: a leader cut off from
  Redis stops renewing and gets superseded, but the code has no path where it notices and
  steps down.
- **Redis failure is untested**, and is a single point of failure by design.
- **Malformed input is untested.** No test sends a truncated frame, invalid JSON, an
  unknown message type, a negative or out-of-range offset, or a CSV with missing columns.
  Some of these are handled in code — `receive_message()` catches and returns `None`,
  unknown types get an `ERROR` reply, the flight producer validates CSV headers before
  streaming — but none are exercised by a test.
- **Broker restart / rejoin is untested**, because it isn't implemented.
- **Two tests can pass vacuously** (3 and 4) when no data is present.
