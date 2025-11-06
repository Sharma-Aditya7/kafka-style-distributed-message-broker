# Airline Data Streaming Pipeline

Real-time airline data processing using Kafka, MongoDB, and MySQL.

## Team Members
- Person 1: Kafka & Producer
- Person 2: MongoDB Consumer & Analytics
- Person 3: MySQL Consumer & ML Predictions
- Person 4: Integration & Testing

## Quick Start
```bash
# Install dependencies
pip install -r requirements.txt

# Start system
docker-compose up -d
python producer/stream_data.py
```

## Project Structure
- `producer/` - Kafka producer streaming airline data
- `mongodb-consumer/` - MongoDB consumer and analytics
- `mysql-consumer/` - MySQL consumer and predictions
- `ml-model/` - Machine learning model files
- `monitoring/` - System monitoring scripts
- `tests/` - Integration tests
- `docs/` - Documentation
