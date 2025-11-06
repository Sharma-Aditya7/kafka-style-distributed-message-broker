"""
Redis Client Wrapper
Provides utilities for interacting with Redis metadata store
"""
import redis
from typing import Optional, Dict, Any
import json


class RedisMetadataStore:
    """Wrapper class for Redis operations"""

    # Redis key constants
    LEADER_CURRENT = "leader:current"
    LEADER_LEASE = "leader:lease"
    HWM_OFFSET = "hwm:offset"
    CONSUMER_OFFSET = "consumer:offset"

    def __init__(self, host: str = 'localhost', port: int = 6379, db: int = 0):
        """Initialize Redis connection"""
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        try:
            self.client.ping()
            print(f"Connected to Redis at {host}:{port}")
        except redis.ConnectionError as e:
            print(f"Failed to connect to Redis: {e}")
            raise

    def set_leader(self, host: str, port: int, lease_ttl: int = 30) -> bool:
        """
        Atomically set the current leader with a lease TTL
        Returns True if successful, False if another leader exists
        """
        leader_info = json.dumps({'host': host, 'port': port})

        # Use SETNX (SET if Not eXists) with expiration for atomic leader election
        # Returns 1 if key was set, 0 if key already exists
        result = self.client.set(self.LEADER_LEASE, leader_info, nx=True, ex=lease_ttl)

        if result:
            # Also update the current leader info (without expiration)
            self.client.set(self.LEADER_CURRENT, leader_info)
            return True
        return False

    def renew_leader_lease(self, host: str, port: int, lease_ttl: int = 30) -> bool:
        """
        Renew the leader lease (used by current leader to maintain authority)
        Returns True if successful
        """
        leader_info = json.dumps({'host': host, 'port': port})

        # Update the lease with new TTL
        self.client.set(self.LEADER_LEASE, leader_info, ex=lease_ttl)
        self.client.set(self.LEADER_CURRENT, leader_info)
        return True

    def get_leader(self) -> Optional[Dict[str, Any]]:
        """
        Get current leader information
        Returns dict with 'host' and 'port' or None if no leader
        """
        leader_info = self.client.get(self.LEADER_CURRENT)
        if leader_info:
            return json.loads(leader_info)
        return None

    def check_leader_lease_exists(self) -> bool:
        """
        Check if a leader lease currently exists
        Returns True if lease is active, False otherwise
        """
        return self.client.exists(self.LEADER_LEASE) > 0

    def release_leader(self) -> None:
        """Release leader lease (used during graceful shutdown)"""
        self.client.delete(self.LEADER_LEASE)
        self.client.delete(self.LEADER_CURRENT)

    def set_hwm(self, offset: int) -> None:
        """Set the High Water Mark (highest replicated offset)"""
        self.client.set(self.HWM_OFFSET, offset)

    def get_hwm(self) -> int:
        """Get the High Water Mark"""
        hwm = self.client.get(self.HWM_OFFSET)
        return int(hwm) if hwm else -1

    def set_consumer_offset(self, consumer_id: str, offset: int) -> None:
        """Set the consumer's last read offset"""
        key = f"{self.CONSUMER_OFFSET}:{consumer_id}"
        self.client.set(key, offset)

    def get_consumer_offset(self, consumer_id: str) -> int:
        """Get the consumer's last read offset"""
        key = f"{self.CONSUMER_OFFSET}:{consumer_id}"
        offset = self.client.get(key)
        return int(offset) if offset else -1

    def close(self) -> None:
        """Close Redis connection"""
        self.client.close()
