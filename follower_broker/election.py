"""
Leader Election Manager
Monitors leader health and performs leader election when needed
"""
import time
import threading
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.redis_client import RedisMetadataStore
from common.config import Config


class ElectionManager:
    """Manages leader election and monitoring"""

    def __init__(self, redis_store: RedisMetadataStore, host: str, port: int, on_become_leader_callback):
        self.redis_store = redis_store
        self.host = host
        self.port = port
        self.on_become_leader_callback = on_become_leader_callback

        self.monitoring = False
        self.monitor_thread = None

    def monitor_leader_health(self):
        """Monitor leader lease and attempt election if leader fails"""
        consecutive_failures = 0
        required_failures = 3  # Number of consecutive checks before attempting election

        print("Started monitoring leader health...")

        while self.monitoring:
            try:
                # Check if leader lease exists
                lease_exists = self.redis_store.check_leader_lease_exists()

                if lease_exists:
                    # Leader is alive
                    consecutive_failures = 0
                    current_leader = self.redis_store.get_leader()
                    if current_leader:
                        print(f"Leader alive: {current_leader['host']}:{current_leader['port']}")
                else:
                    # Leader lease expired
                    consecutive_failures += 1
                    print(f"⚠ Leader lease not found (attempt {consecutive_failures}/{required_failures})")

                    if consecutive_failures >= required_failures:
                        print("Leader failure detected! Attempting election...")
                        self.attempt_leader_election()
                        consecutive_failures = 0  # Reset after election attempt

            except Exception as e:
                print(f"Error monitoring leader: {e}")

            # Wait before next check
            time.sleep(Config.HEARTBEAT_INTERVAL)

    def attempt_leader_election(self):
        """Attempt to become leader using atomic Redis operation"""
        try:
            print(f"Attempting to acquire leadership...")

            # Try to atomically acquire leader lease using SETNX
            success = self.redis_store.set_leader(self.host, self.port, Config.LEADER_LEASE_TTL)

            if success:
                print(f"✓✓✓ LEADERSHIP ACQUIRED! ✓✓✓")
                print(f"New leader: {self.host}:{self.port}")
                print("=" * 60)

                # Notify the broker that we're now the leader
                if self.on_become_leader_callback:
                    self.on_become_leader_callback()

            else:
                print("✗ Election failed - another follower became leader")

        except Exception as e:
            print(f"Error during leader election: {e}")

    def start_monitoring(self):
        """Start monitoring leader health"""
        self.monitoring = True
        self.monitor_thread = threading.Thread(target=self.monitor_leader_health, daemon=True)
        self.monitor_thread.start()
        print("Leader monitoring started")

    def stop_monitoring(self):
        """Stop monitoring leader health"""
        self.monitoring = False
        if self.monitor_thread:
            self.monitor_thread.join(timeout=5)
        print("Leader monitoring stopped")
