#!/bin/bash
# Start Follower Broker (Node 2)

echo "=========================================="
echo "Starting Follower Broker (Node 2)"
echo "=========================================="

# Configuration
FOLLOWER_HOST="${FOLLOWER_HOST:-0.0.0.0}"
FOLLOWER_PORT="${FOLLOWER_PORT:-9093}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"

# Navigate to project root
cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Start follower broker
python3 -m follower_broker.follower \
    --host "$FOLLOWER_HOST" \
    --port "$FOLLOWER_PORT" \
    --redis-host "$REDIS_HOST" \
    --redis-port "$REDIS_PORT"
