"""
Follower Broker Implementation
Handles replication from leader and can promote to leader on failure
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
from follower_broker.log_manager import LogManager
from follower_broker.election import ElectionManager


class FollowerBroker:
    """Follower Broker that replicates from leader and can become leader"""

    def __init__(self, host: str, port: int, redis_host: str, redis_port: int):
        self.host = host
        self.port = port

        # Initialize components
        self.log_manager = LogManager()
        self.redis_store = RedisMetadataStore(redis_host, redis_port)
        self.election_manager = ElectionManager(
            self.redis_store,
            self.host,
            self.port,
            self.on_become_leader
        )

        self.server_socket = None
        self.is_leader = False
        self.running = False

        # Threads
        self.accept_thread = None
        self.lease_thread = None

    def on_become_leader(self):
        """Callback when this follower becomes the leader"""
        print("🎉 PROMOTION TO LEADER! 🎉")
        self.is_leader = True

        # Start leader lease renewal
        self.lease_thread = threading.Thread(target=self.renew_lease_loop, daemon=True)
        self.lease_thread.start()

        print("✓ Now acting as leader - accepting producer requests")

    def renew_lease_loop(self):
        """Background thread to continuously renew leader lease (when promoted)"""
        while self.running and self.is_leader:
            try:
                self.redis_store.renew_leader_lease(self.host, self.port, Config.LEADER_LEASE_TTL)
                print(f"Leader lease renewed (TTL: {Config.LEADER_LEASE_TTL}s)")
            except Exception as e:
                print(f"Error renewing lease: {e}")

            time.sleep(Config.HEARTBEAT_INTERVAL)

    def handle_replicate_request(self, client_socket: socket.socket, message: Message):
        """Handle REPLICATE request from leader"""
        try:
            # Append message to local log
            offset = self.log_manager.append(
                message.data,
                message.message_id,
                message.timestamp,
                message.offset
            )

            if offset is not None:
                # Send ACK back to leader
                ack_msg = create_ack_message(offset, success=True)
                send_message(client_socket, ack_msg)
                print(f"✓ Replicated message at offset {offset}")
            else:
                # Duplicate message
                ack_msg = create_ack_message(message.offset, success=True)
                send_message(client_socket, ack_msg)
                print(f"✓ Duplicate message at offset {message.offset} - ACK sent")

        except Exception as e:
            print(f"Error handling replication: {e}")
            error_ack = create_ack_message(-1, success=False, error_msg=str(e))
            try:
                send_message(client_socket, error_ack)
            except:
                pass

    def handle_produce_request(self, client_socket: socket.socket, message: Message):
        """Handle PRODUCE request - only allowed if we're the leader"""
        try:
            if not self.is_leader:
                # Reject - we're not the leader
                error_msg = create_error_message("NOT_THE_LEADER", "NOT_LEADER")
                send_message(client_socket, error_msg)
                print("⚠ Rejected PRODUCE request - not the leader")
                return

            # If we're the leader (after promotion), handle like leader broker
            # For simplicity, we'll accept the message but won't replicate (no follower setup in this version)
            offset = self.log_manager.append(message.data, message.message_id, message.timestamp)

            if offset is not None:
                # Update HWM
                self.redis_store.set_hwm(offset)

                # Send ACK
                ack_msg = create_ack_message(offset, success=True)
                send_message(client_socket, ack_msg)
                print(f"✓ Message committed at offset {offset} (as leader)")
            else:
                # Duplicate
                error_msg = create_error_message("Duplicate message", "DUPLICATE")
                send_message(client_socket, error_msg)

        except Exception as e:
            print(f"Error handling produce request: {e}")
            error_msg = create_error_message(str(e))
            try:
                send_message(client_socket, error_msg)
            except:
                pass

    def handle_fetch_request(self, client_socket: socket.socket, message: Message):
        """Handle FETCH request from consumer (only if we're the leader)"""
        try:
            if not self.is_leader:
                error_msg = create_error_message("NOT_THE_LEADER", "NOT_LEADER")
                send_message(client_socket, error_msg)
                return

            start_offset = message.data.get('start_offset', 0)
            max_messages = message.data.get('max_messages', Config.FETCH_MAX_MESSAGES)

            # Get HWM
            hwm = self.redis_store.get_hwm()
            print(f"Fetch request: start_offset={start_offset}, HWM={hwm}")

            # Retrieve messages up to HWM
            messages = self.log_manager.get_messages(start_offset, max_messages)
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

                if message.type == MessageType.REPLICATE:
                    self.handle_replicate_request(client_socket, message)

                elif message.type == MessageType.PRODUCE:
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
        """Accept incoming connections"""
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
        """Start the follower broker"""
        print("=" * 60)
        print("Starting Follower Broker")
        print("=" * 60)

        # Start server socket
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(5)

        print(f"Follower broker listening on {self.host}:{self.port}")
        self.running = True

        # Start leader monitoring
        self.election_manager.start_monitoring()

        # Start accepting connections
        self.accept_thread = threading.Thread(target=self.accept_connections, daemon=True)
        self.accept_thread.start()

        print("✓ Follower broker started successfully")
        print("=" * 60)

        # Keep main thread alive
        try:
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("\nShutting down follower broker...")
            self.stop()

    def stop(self):
        """Stop the follower broker"""
        self.running = False

        # Stop monitoring
        self.election_manager.stop_monitoring()

        # Release leadership if we have it
        if self.is_leader:
            try:
                self.redis_store.release_leader()
            except:
                pass

        # Close connections
        if self.server_socket:
            self.server_socket.close()

        self.redis_store.close()

        print("Follower broker stopped")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Follower Broker')
    parser.add_argument('--host', default=Config.FOLLOWER_HOST, help='Follower host')
    parser.add_argument('--port', type=int, default=Config.FOLLOWER_PORT, help='Follower port')
    parser.add_argument('--redis-host', default=Config.REDIS_HOST, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=Config.REDIS_PORT, help='Redis port')

    args = parser.parse_args()

    broker = FollowerBroker(
        args.host,
        args.port,
        args.redis_host,
        args.redis_port
    )
    broker.start()


if __name__ == '__main__':
    main()
