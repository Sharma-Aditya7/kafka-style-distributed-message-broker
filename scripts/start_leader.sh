#!/bin/bash
# Start Leader Broker (Node 1)

echo "=========================================="
echo "Starting Leader Broker (Node 1)"
echo "=========================================="

# Configuration
LEADER_HOST="${LEADER_HOST:-0.0.0.0}"
LEADER_PORT="${LEADER_PORT:-9092}"
FOLLOWER_HOST="${FOLLOWER_HOST:-localhost}"
FOLLOWER_PORT="${FOLLOWER_PORT:-9093}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"

# Navigate to project root
cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Start leader broker
python3 -m leader_broker.leader \
    --host "$LEADER_HOST" \
    --port "$LEADER_PORT" \
    --follower-host "$FOLLOWER_HOST" \
    --follower-port "$FOLLOWER_PORT" \
    --redis-host "$REDIS_HOST" \
    --redis-port "$REDIS_PORT"
