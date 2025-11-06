"""
Configuration Management
Shared configuration settings for all nodes
"""
import os


class Config:
    """Configuration class with default values"""

    # Redis Configuration
    REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

    # Leader Broker Configuration
    LEADER_HOST = os.getenv('LEADER_HOST', '0.0.0.0')
    LEADER_PORT = int(os.getenv('LEADER_PORT', 9092))

    # Follower Broker Configuration
    FOLLOWER_HOST = os.getenv('FOLLOWER_HOST', '0.0.0.0')
    FOLLOWER_PORT = int(os.getenv('FOLLOWER_PORT', 9093))

    # Leader Lease Configuration
    LEADER_LEASE_TTL = int(os.getenv('LEADER_LEASE_TTL', 30))  # seconds
    HEARTBEAT_INTERVAL = int(os.getenv('HEARTBEAT_INTERVAL', 5))  # seconds
    HEARTBEAT_TIMEOUT = int(os.getenv('HEARTBEAT_TIMEOUT', 15))  # seconds (3 missed heartbeats)

    # Replication Configuration
    REPLICATION_TIMEOUT = int(os.getenv('REPLICATION_TIMEOUT', 10))  # seconds
    MAX_BATCH_SIZE = int(os.getenv('MAX_BATCH_SIZE', 100))  # messages

    # Consumer Configuration
    DEFAULT_CONSUMER_ID = os.getenv('CONSUMER_ID', 'consumer-1')
    FETCH_MAX_MESSAGES = int(os.getenv('FETCH_MAX_MESSAGES', 100))

    # Network Configuration
    SOCKET_TIMEOUT = int(os.getenv('SOCKET_TIMEOUT', 30))  # seconds
    RECONNECT_DELAY = int(os.getenv('RECONNECT_DELAY', 2))  # seconds
    MAX_RETRIES = int(os.getenv('MAX_RETRIES', 3))

    @staticmethod
    def print_config():
        """Print current configuration"""
        print("=" * 60)
        print("YAK Message Broker Configuration")
        print("=" * 60)
        print(f"Redis: {Config.REDIS_HOST}:{Config.REDIS_PORT}")
        print(f"Leader Broker: {Config.LEADER_HOST}:{Config.LEADER_PORT}")
        print(f"Follower Broker: {Config.FOLLOWER_HOST}:{Config.FOLLOWER_PORT}")
        print(f"Leader Lease TTL: {Config.LEADER_LEASE_TTL}s")
        print(f"Heartbeat Interval: {Config.HEARTBEAT_INTERVAL}s")
        print("=" * 60)
