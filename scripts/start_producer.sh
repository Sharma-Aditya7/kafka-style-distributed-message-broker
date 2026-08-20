#!/bin/bash
# Start Producer Client (Node 3)

echo "=========================================="
echo "Starting Producer Client (Node 3)"
echo "=========================================="

# Configuration
BROKERS="${BROKERS:-localhost:9092 localhost:9093}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"

# Navigate to project root
cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Start producer in interactive mode
python3 -m producer.producer \
    --brokers $BROKERS \
    --redis-host "$REDIS_HOST" \
    --redis-port "$REDIS_PORT"
