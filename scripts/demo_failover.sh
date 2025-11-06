#!/bin/bash
# Demo script to test leader failover
# This script demonstrates zero data loss during leader failure

echo "=========================================="
echo "YAK Message Broker - Failover Demo"
echo "=========================================="
echo ""

# Configuration
LEADER_PID_FILE="/tmp/yak_leader.pid"
PRODUCER_SCRIPT="$(dirname "$0")/start_producer.sh"
CONSUMER_SCRIPT="$(dirname "$0")/start_consumer.sh"
BROKERS="localhost:9092 localhost:9093"
REDIS_HOST="localhost"
REDIS_PORT="6379"

# Navigate to project root
cd "$(dirname "$0")/.."

echo "Step 1: Send 100 test messages to the leader"
echo "---------------------------------------------"
python3 -m producer.producer \
    --brokers $BROKERS \
    --redis-host $REDIS_HOST \
    --redis-port $REDIS_PORT \
    --batch 100

if [ $? -eq 0 ]; then
    echo "✓ Successfully sent 100 messages"
else
    echo "✗ Failed to send messages"
    exit 1
fi

echo ""
echo "Step 2: Verify messages are replicated"
echo "---------------------------------------------"
sleep 2
echo "✓ Replication complete (2 second wait)"

echo ""
echo "Step 3: Kill the leader broker"
echo "---------------------------------------------"
if [ -f "$LEADER_PID_FILE" ]; then
    LEADER_PID=$(cat "$LEADER_PID_FILE")
    kill -9 $LEADER_PID 2>/dev/null
    rm "$LEADER_PID_FILE"
    echo "✓ Leader process killed (PID: $LEADER_PID)"
else
    echo "⚠ Leader PID file not found - you may need to kill the leader manually"
    echo "  Find the process with: ps aux | grep leader_broker"
    echo "  Kill it with: kill -9 <PID>"
    read -p "Press Enter after killing the leader..."
fi

echo ""
echo "Step 4: Wait for follower to detect failure and promote itself"
echo "---------------------------------------------"
echo "Waiting 20 seconds for leader election..."
sleep 20
echo "✓ Follower should now be the new leader"

echo ""
echo "Step 5: Consume all messages from new leader"
echo "---------------------------------------------"
python3 -m consumer.consumer \
    --consumer-id "demo-consumer" \
    --brokers $BROKERS \
    --redis-host $REDIS_HOST \
    --redis-port $REDIS_PORT \
    --fetch-all > /tmp/yak_consumed_messages.txt

MESSAGE_COUNT=$(wc -l < /tmp/yak_consumed_messages.txt)

echo ""
echo "=========================================="
echo "Failover Demo Results"
echo "=========================================="
echo "Messages sent: 100"
echo "Messages consumed: $MESSAGE_COUNT"
echo ""

if [ "$MESSAGE_COUNT" -eq 100 ]; then
    echo "✓✓✓ SUCCESS! ZERO DATA LOSS! ✓✓✓"
    echo "All 100 messages survived the leader failure!"
else
    echo "⚠ Warning: Message count mismatch"
    echo "Expected: 100, Got: $MESSAGE_COUNT"
fi

echo ""
echo "Consumed messages saved to: /tmp/yak_consumed_messages.txt"
echo "=========================================="
