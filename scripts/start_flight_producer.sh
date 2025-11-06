#!/bin/bash
# Start Flight Data Producer (Node 3)

echo "=========================================="
echo "Starting Flight Data Producer"
echo "=========================================="

# Configuration
BROKERS="${BROKERS:-localhost:9092 localhost:9093}"
REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
CSV_FILE="${CSV_FILE:-data/FlightDelay2.csv}"
DELAY="${DELAY:-100}"  # milliseconds between records
MAX_RECORDS="${MAX_RECORDS:-}"  # empty = all records

# Navigate to project root
cd "$(dirname "$0")/.."

# Activate virtual environment if it exists
if [ -d "venv" ]; then
    source venv/bin/activate
fi

# Build command
CMD="python3 -m producer.flight_data_producer \
    --csv $CSV_FILE \
    --brokers $BROKERS \
    --redis-host $REDIS_HOST \
    --redis-port $REDIS_PORT \
    --delay $DELAY"

# Add max records if specified
if [ ! -z "$MAX_RECORDS" ]; then
    CMD="$CMD --max-records $MAX_RECORDS"
fi

# Run producer
echo "Configuration:"
echo "  CSV File: $CSV_FILE"
echo "  Brokers: $BROKERS"
echo "  Redis: $REDIS_HOST:$REDIS_PORT"
echo "  Delay: ${DELAY}ms"
echo "  Max Records: ${MAX_RECORDS:-All}"
echo ""

$CMD
