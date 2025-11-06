#!/bin/bash
# Start Spark Analytics Job

echo "=========================================="
echo "Starting Spark Flight Delay Analytics"
echo "=========================================="

# Configuration
BROKERS="${BROKERS:-localhost:9092 localhost:9093}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
CONSUMER_ID="${CONSUMER_ID:-spark-analytics-consumer}"
OUTPUT="${OUTPUT:-output/flight_analysis}"
SAVE="${SAVE:-true}"

# Navigate to project root
cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Build command
CMD="python3 -m spark_jobs.flight_delay_streaming \
    --brokers $BROKERS \
    --redis-host $REDIS_HOST \
    --redis-port $REDIS_PORT \
    --consumer-id $CONSUMER_ID \
    --output $OUTPUT"

# Add save flag if enabled
if [ "$SAVE" = "true" ]; then
    CMD="$CMD --save"
fi

# Run Spark job
echo "Configuration:"
echo "  Brokers: $BROKERS"
echo "  Redis: $REDIS_HOST:$REDIS_PORT"
echo "  Consumer ID: $CONSUMER_ID"
echo "  Output: $OUTPUT"
echo "  Save Results: $SAVE"
echo ""

$CMD
