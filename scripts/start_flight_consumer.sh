#!/bin/bash
# Start Flight Data Consumer (Node 4)

echo "=========================================="
echo "Starting Flight Data Analytics Consumer"
echo "=========================================="

# Configuration
CONSUMER_ID="${CONSUMER_ID:-flight-analytics-consumer}"
BROKERS="${BROKERS:-localhost:9092 localhost:9093}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
MODE="${MODE:-batch}"  # batch or continuous

# Navigate to project root
cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Build command
CMD="python3 -m consumer.flight_data_consumer \
    --consumer-id $CONSUMER_ID \
    --brokers $BROKERS \
    --redis-host $REDIS_HOST \
    --redis-port $REDIS_PORT"

# Add mode flag
if [ "$MODE" = "continuous" ]; then
    CMD="$CMD --continuous"
elif [ "$MODE" = "batch" ]; then
    CMD="$CMD --batch"
fi

# Run consumer
echo "Configuration:"
echo "  Consumer ID: $CONSUMER_ID"
echo "  Brokers: $BROKERS"
echo "  Redis: $REDIS_HOST:$REDIS_PORT"
echo "  Mode: $MODE"
echo ""

$CMD
