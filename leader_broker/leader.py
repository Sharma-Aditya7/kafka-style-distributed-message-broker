"""
Leader Broker Implementation
Handles producer requests, replication, and leader lease management
"""
import socket
import threading
import time
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import (Message, MessageType, send_message, receive_message,
                              create_ack_message, create_error_message, create_metadata_message)
from common.redis_client import RedisMetadataStore
from common.config import Config
from leader_broker.log_manager import LogManager
from leader_broker.replication import ReplicationManager


class LeaderBroker:
    """Leader Broker that handles writes and replication"""

    def __init__(self, host: str, port: int, follower_host: str, follower_port: int, redis_host: str, redis_port: int):
        self.host = host
        self.port = port
        self.follower_host = follower_host
        self.follower_port = follower_port

        # Initialize components
        self.log_manager = LogManager()
        self.redis_store = RedisMetadataStore(redis_host, redis_port)
        self.replication_manager = ReplicationManager(follower_host, follower_port)

        self.server_socket = None
        self.is_leader = False
        self.running = False

        # Threads
        self.lease_thread = None
        self.accept_thread = None

    def acquire_leadership(self) -> bool:
        """Attempt to acquire leader lease"""
        print("Attempting to acquire leadership...")
        success = self.redis_store.set_leader(self.host, self.port, Config.LEADER_LEASE_TTL)

        if success:
            self.is_leader = True
            print(f"✓ Leadership acquired! Leader at {self.host}:{self.port}")
            return True
        else:
            print("✗ Failed to acquire leadership (another leader exists)")
            return False

    def renew_lease_loop(self):
        """Background thread to continuously renew leader lease"""
        while self.running and self.is_leader:
            try:
                self.redis_store.renew_leader_lease(self.host, self.port, Config.LEADER_LEASE_TTL)
                print(f"Leader lease renewed (TTL: {Config.LEADER_LEASE_TTL}s)")
            except Exception as e:
                print(f"Error renewing lease: {e}")

            time.sleep(Config.HEARTBEAT_INTERVAL)

    def handle_produce_request(self, client_socket: socket.socket, message: Message):
        """Handle PRODUCE request from producer"""
        try:
            if not self.is_leader:
                # Reject if not leader
                error_msg = create_error_message("NOT_THE_LEADER", "NOT_LEADER")
                send_message(client_socket, error_msg)
                return

            # Append to local log
            offset = self.log_manager.append(message.data, message.message_id, message.timestamp)

            if offset is None:
                # Duplicate message - still return success with existing offset
                print(f"Duplicate message {message.message_id} - returning existing offset")
                # Find the offset
                for i in range(self.log_manager.get_message_count()):
                    msg = self.log_manager.get_message_at_offset(i)
                    if msg and msg.get('message_id') == message.message_id:
                        offset = i
                        break

            # Replicate to follower synchronously
            replication_success = self.replication_manager.replicate_message(
                message.data, offset, message.message_id
            )

            if not replication_success:
                print(f"⚠ Replication failed for offset {offset}")
                error_msg = create_error_message("Replication failed", "REPLICATION_ERROR")
                send_message(client_socket, error_msg)
                return

            # Update High Water Mark (HWM) after successful replication
            self.redis_store.set_hwm(offset)
            print(f"HWM updated to {offset}")

            # Send ACK to producer
            ack_msg = create_ack_message(offset, success=True)
            send_message(client_socket, ack_msg)
            print(f"✓ Message committed at offset {offset}")

        except Exception as e:
            print(f"Error handling produce request: {e}")
            error_msg = create_error_message(str(e))
            try:
                send_message(client_socket, error_msg)
            except:
                pass

    def handle_fetch_request(self, client_socket: socket.socket, message: Message):
        """Handle FETCH request from consumer"""
        try:
            start_offset = message.data.get('start_offset', 0)
            max_messages = message.data.get('max_messages', Config.FETCH_MAX_MESSAGES)

            # Get HWM - consumers can only read up to HWM
            hwm = self.redis_store.get_hwm()
            print(f"Fetch request: start_offset={start_offset}, HWM={hwm}")

            # Retrieve messages up to HWM
            messages = self.log_manager.get_messages(start_offset, max_messages)

            # Filter messages beyond HWM
            filtered_messages = [msg for msg in messages if msg['offset'] <= hwm]

            # Send response
            response = Message(
                msg_type=MessageType.ACK,
                data={'messages': filtered_messages, 'hwm': hwm},
                offset=hwm
            )
            send_message(client_socket, response)
            print(f"✓ Sent {len(filtered_messages)} messages to consumer")

        except Exception as e:
            print(f"Error handling fetch request: {e}")
            error_msg = create_error_message(str(e))
            try:
                send_message(client_socket, error_msg)
            except:
                pass

    def handle_metadata_request(self, client_socket: socket.socket):
        """Handle METADATA request - return current leader info"""
        try:
            leader_info = self.redis_store.get_leader()
            if leader_info:
                metadata_msg = create_metadata_message(
                    leader_info['host'],
                    leader_info['port']
                )
            else:
                metadata_msg = create_error_message("No leader available", "NO_LEADER")

            send_message(client_socket, metadata_msg)

        except Exception as e:
            print(f"Error handling metadata request: {e}")

    def handle_client(self, client_socket: socket.socket, client_address):
        """Handle client connection"""
        print(f"Client connected: {client_address}")

        try:
            while self.running:
                message = receive_message(client_socket, timeout=30)

                if not message:
                    break

                print(f"Received {message.type} request")

                if message.type == MessageType.PRODUCE:
                    self.handle_produce_request(client_socket, message)

                elif message.type == MessageType.FETCH:
                    self.handle_fetch_request(client_socket, message)

                elif message.type == MessageType.METADATA:
                    self.handle_metadata_request(client_socket)

                else:
                    error_msg = create_error_message(f"Unknown message type: {message.type}")
                    send_message(client_socket, error_msg)

        except Exception as e:
            print(f"Error handling client {client_address}: {e}")

        finally:
            client_socket.close()
            print(f"Client disconnected: {client_address}")

    def accept_connections(self):
        """Accept incoming client connections"""
        while self.running:
            try:
                client_socket, client_address = self.server_socket.accept()
                client_thread = threading.Thread(
                    target=self.handle_client,
                    args=(client_socket, client_address),
                    daemon=True
                )
                client_thread.start()

            except Exception as e:
                if self.running:
                    print(f"Error accepting connection: {e}")

    def start(self):
        """Start the leader broker"""
        print("=" * 60)
        print("Starting Leader Broker")
        print("=" * 60)

        # Acquire leadership
        if not self.acquire_leadership():
            print("Cannot start as leader - another leader exists")
            return

        # Start server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)

        print(f"Leader broker listening on {self.host}:{self.port}")
        self.running = True

        # Start lease renewal thread
        self.lease_thread = threading.Thread(target=self.renew_lease_loop, daemon=True)
        self.lease_thread.start()

        # Start accepting connections
        self.accept_thread = threading.Thread(target=self.accept_connections, daemon=True)
        self.accept_thread.start()

        print("✓ Leader broker started successfully")
        print("=" * 60)

        # Keep main thread alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down leader broker...")
            self.stop()

    def stop(self):
        """Stop the leader broker"""
        self.running = False
        self.is_leader = False

        # Release leadership
        try:
            self.redis_store.release_leader()
        except:
            pass

        # Close connections
        if self.server_socket:
            self.server_socket.close()

        self.replication_manager.close()
        self.redis_store.close()

        print("Leader broker stopped")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Leader Broker')
    parser.add_argument('--host', default=Config.LEADER_HOST, help='Leader host')
    parser.add_argument('--port', type=int, default=Config.LEADER_PORT, help='Leader port')
    parser.add_argument('--follower-host', required=True, help='Follower host')
    parser.add_argument('--follower-port', type=int, default=Config.FOLLOWER_PORT, help='Follower port')
    parser.add_argument('--redis-host', default=Config.REDIS_HOST, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=Config.REDIS_PORT, help='Redis port')

    args = parser.parse_args()

    broker = LeaderBroker(
        args.host,
        args.port,
        args.follower_host,
        args.follower_port,
        args.redis_host,
        args.redis_port
    )
    broker.start()


if __name__ == '__main__':
    main()
