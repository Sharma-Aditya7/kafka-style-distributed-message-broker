"""
Replication Manager
Handles synchronous replication to follower broker
"""
import socket
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import Message, MessageType, send_message, receive_message, create_replicate_message
from common.config import Config


class ReplicationManager:
    """Manages replication to follower broker"""

    def __init__(self, follower_host: str, follower_port: int):
        self.follower_host = follower_host
        self.follower_port = follower_port
        self.socket = None

    def connect_to_follower(self) -> bool:
        """Establish connection to follower broker"""
        try:
            if self.socket:
                self.socket.close()

            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(Config.REPLICATION_TIMEOUT)
            self.socket.connect((self.follower_host, self.follower_port))
            print(f"Connected to follower at {self.follower_host}:{self.follower_port}")
            return True

        except Exception as e:
            print(f"Failed to connect to follower: {e}")
            self.socket = None
            return False

    def replicate_message(self, data: Any, offset: int, message_id: str) -> bool:
        """
        Synchronously replicate a message to follower
        Returns True if ACK received, False otherwise
        """
        max_retries = 3
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Ensure connection is established
                if not self.socket:
                    if not self.connect_to_follower():
                        retry_count += 1
                        continue

                # Create and send replication message
                replicate_msg = create_replicate_message(data, offset, message_id)
                send_message(self.socket, replicate_msg)
                print(f"Sent replication request for offset {offset}")

                # Wait for ACK from follower
                ack_msg = receive_message(self.socket, timeout=Config.REPLICATION_TIMEOUT)

                if ack_msg and ack_msg.type == MessageType.ACK:
                    if ack_msg.data and ack_msg.data.get('success'):
                        print(f"Received ACK for offset {offset}")
                        return True
                    else:
                        error = ack_msg.data.get('error', 'Unknown error')
                        print(f"Follower rejected replication: {error}")
                        return False
                else:
                    print(f"Invalid ACK received from follower")
                    self.socket = None
                    retry_count += 1

            except Exception as e:
                print(f"Replication error: {e}")
                self.socket = None
                retry_count += 1

        print(f"Replication failed after {max_retries} retries")
        return False

    def close(self):
        """Close connection to follower"""
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
            self.socket = None
