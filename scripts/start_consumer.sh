#!/bin/bash
# Start Consumer Client (Node 4)

echo "=========================================="
echo "Starting Consumer Client (Node 4)"
echo "=========================================="

# Configuration
CONSUMER_ID="${CONSUMER_ID:-consumer-1}"
BROKERS="${BROKERS:-localhost:9092 localhost:9093}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"

# Navigate to project root
cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Start consumer in interactive mode
python3 -m consumer.consumer \
    --consumer-id "$CONSUMER_ID" \
    --brokers $BROKERS \
    --redis-host "$REDIS_HOST" \
    --redis-port "$REDIS_PORT"
