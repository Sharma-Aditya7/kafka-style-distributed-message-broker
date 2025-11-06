"""
Consumer Client Implementation
Reads messages from the leader broker with automatic failover
"""
import socket
import time
import sys
import os
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import (Message, MessageType, send_message, receive_message,
                              create_fetch_message)
from common.redis_client import RedisMetadataStore
from common.config import Config


class Consumer:
    """Consumer client that reads messages from the broker"""

    def __init__(self, consumer_id: str, broker_addresses: list, redis_host: str, redis_port: int):
        """
        Initialize consumer
        consumer_id: unique identifier for this consumer
        broker_addresses: list of (host, port) tuples for all known brokers
        """
        self.consumer_id = consumer_id
        self.broker_addresses = broker_addresses
        self.redis_store = RedisMetadataStore(redis_host, redis_port)

        self.current_leader = None
        self.socket = None
        self.current_offset = -1

        # Load last committed offset
        self.load_offset()

    def load_offset(self):
        """Load the last committed offset from Redis"""
        try:
            offset = self.redis_store.get_consumer_offset(self.consumer_id)
            self.current_offset = offset
            print(f"Loaded offset: {self.current_offset}")
        except Exception as e:
            print(f"Could not load offset: {e}")
            self.current_offset = -1

    def commit_offset(self, offset: int):
        """Commit the current offset to Redis"""
        try:
            self.redis_store.set_consumer_offset(self.consumer_id, offset)
            self.current_offset = offset
            print(f"Committed offset: {offset}")
        except Exception as e:
            print(f"Error committing offset: {e}")

    def discover_leader(self) -> tuple:
        """
        Discover the current leader from Redis metadata
        Returns (host, port) tuple or None
        """
        try:
            leader_info = self.redis_store.get_leader()
            if leader_info:
                print(f"Discovered leader: {leader_info['host']}:{leader_info['port']}")
                return (leader_info['host'], leader_info['port'])

        except Exception as e:
            print(f"Error discovering leader from Redis: {e}")

        # Fallback: try asking brokers directly
        print("Trying to discover leader by querying brokers...")
        for host, port in self.broker_addresses:
            try:
                temp_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                temp_socket.settimeout(5)
                temp_socket.connect((host, port))

                # Request metadata
                metadata_msg = Message(MessageType.METADATA)
                send_message(temp_socket, metadata_msg)

                response = receive_message(temp_socket, timeout=5)
                temp_socket.close()

                if response and response.type == MessageType.METADATA:
                    leader_host = response.data.get('leader_host')
                    leader_port = response.data.get('leader_port')
                    print(f"Discovered leader from broker: {leader_host}:{leader_port}")
                    return (leader_host, leader_port)

            except Exception as e:
                print(f"Failed to query broker {host}:{port}: {e}")
                continue

        return None

    def connect_to_leader(self) -> bool:
        """Establish connection to the current leader"""
        try:
            # Discover leader if not known
            if not self.current_leader:
                self.current_leader = self.discover_leader()

            if not self.current_leader:
                print("✗ Could not discover leader")
                return False

            # Close existing connection
            if self.socket:
                try:
                    self.socket.close()
                except:
                    pass

            # Connect to leader
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(Config.SOCKET_TIMEOUT)
            self.socket.connect(self.current_leader)

            print(f"✓ Connected to leader at {self.current_leader[0]}:{self.current_leader[1]}")
            return True

        except Exception as e:
            print(f"Failed to connect to leader: {e}")
            self.socket = None
            self.current_leader = None
            return False

    def fetch_messages(self, max_messages: int = 100) -> list:
        """
        Fetch messages from the broker starting from current offset
        Returns list of messages or None on error
        """
        max_retries = Config.MAX_RETRIES
        retry_count = 0

        while retry_count < max_retries:
            try:
                # Ensure connection to leader
                if not self.socket:
                    if not self.connect_to_leader():
                        retry_count += 1
                        time.sleep(Config.RECONNECT_DELAY)
                        continue

                # Create and send FETCH message
                start_offset = self.current_offset + 1
                fetch_msg = create_fetch_message(start_offset, max_messages)
                send_message(self.socket, fetch_msg)
                print(f"Fetching messages from offset {start_offset}...")

                # Wait for response
                response = receive_message(self.socket, timeout=Config.SOCKET_TIMEOUT)

                if not response:
                    print("No response from broker")
                    self.socket = None
                    self.current_leader = None
                    retry_count += 1
                    time.sleep(Config.RECONNECT_DELAY)
                    continue

                if response.type == MessageType.ACK:
                    messages = response.data.get('messages', [])
                    hwm = response.data.get('hwm', -1)
                    print(f"✓ Received {len(messages)} messages (HWM: {hwm})")
                    return messages

                elif response.type == MessageType.ERROR:
                    error_type = response.data.get('error_type')
                    error_msg = response.data.get('error')

                    if error_type == "NOT_LEADER":
                        print(f"⚠ Broker is not the leader - discovering new leader...")
                        self.socket = None
                        self.current_leader = None
                        retry_count += 1
                        time.sleep(Config.RECONNECT_DELAY)
                        continue
                    else:
                        print(f"✗ Error from broker: {error_msg}")
                        return []

                else:
                    print(f"Unexpected response type: {response.type}")
                    retry_count += 1
                    time.sleep(Config.RECONNECT_DELAY)

            except Exception as e:
                print(f"Error fetching messages: {e}")
                self.socket = None
                self.current_leader = None
                retry_count += 1
                time.sleep(Config.RECONNECT_DELAY)

        print(f"✗ Failed to fetch messages after {max_retries} retries")
        return []

    def consume_messages(self, callback=None, poll_interval: int = 2, continuous: bool = True):
        """
        Continuously consume messages from the broker
        callback: function to call for each message (default: print)
        poll_interval: seconds to wait between polls
        continuous: if True, keep polling; if False, fetch once and exit
        """
        if callback is None:
            callback = lambda msg: print(f"[Offset {msg['offset']}] {msg['data']}")

        print(f"\nStarting consumer (ID: {self.consumer_id})")
        print(f"Current offset: {self.current_offset}")
        print("Press Ctrl+C to stop\n")

        try:
            while True:
                messages = self.fetch_messages()

                if messages:
                    for msg in messages:
                        callback(msg)
                        # Commit offset after processing each message
                        self.commit_offset(msg['offset'])

                    print(f"Processed {len(messages)} messages\n")
                else:
                    print("No new messages available")

                if not continuous:
                    break

                time.sleep(poll_interval)

        except KeyboardInterrupt:
            print("\n\nStopping consumer...")
        finally:
            print(f"Final offset: {self.current_offset}")

    def get_all_messages(self) -> list:
        """
        Fetch all available messages from the beginning
        Returns list of all messages
        """
        print("Fetching all messages from the beginning...")

        # Temporarily set offset to -1 to fetch from beginning
        original_offset = self.current_offset
        self.current_offset = -1

        all_messages = []
        max_iterations = 100  # Prevent infinite loop

        for _ in range(max_iterations):
            messages = self.fetch_messages()

            if not messages:
                break

            all_messages.extend(messages)

            # Update offset to last fetched message
            if messages:
                self.current_offset = messages[-1]['offset']

        # Restore original offset
        self.current_offset = original_offset

        print(f"✓ Fetched {len(all_messages)} total messages")
        return all_messages

    def close(self):
        """Close connection to broker"""
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        self.redis_store.close()


def interactive_mode(consumer: Consumer):
    """Interactive mode for consuming messages"""
    print("\n" + "=" * 60)
    print("Consumer Interactive Mode")
    print("=" * 60)
    print("Commands:")
    print("  start     - Start continuous consumption")
    print("  fetch     - Fetch next batch of messages")
    print("  all       - Fetch all messages from beginning")
    print("  offset    - Show current offset")
    print("  reset     - Reset offset to -1 (read from beginning)")
    print("  quit/exit - Stop consumer")
    print("=" * 60 + "\n")

    while True:
        try:
            command = input("Consumer > ").strip().lower()

            if command in ['quit', 'exit']:
                print("Exiting consumer...")
                break

            elif command == 'start':
                consumer.consume_messages(continuous=True)

            elif command == 'fetch':
                messages = consumer.fetch_messages()
                if messages:
                    for msg in messages:
                        print(f"[Offset {msg['offset']}] {msg['data']}")
                    # Commit last offset
                    consumer.commit_offset(messages[-1]['offset'])
                else:
                    print("No messages available")

            elif command == 'all':
                messages = consumer.get_all_messages()
                if messages:
                    for msg in messages:
                        print(f"[Offset {msg['offset']}] {msg['data']}")
                else:
                    print("No messages available")

            elif command == 'offset':
                print(f"Current offset: {consumer.current_offset}")

            elif command == 'reset':
                consumer.current_offset = -1
                consumer.commit_offset(-1)
                print("Offset reset to -1")

            else:
                print(f"Unknown command: {command}")

        except KeyboardInterrupt:
            print("\nExiting consumer...")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Consumer Client')
    parser.add_argument('--consumer-id', default=Config.DEFAULT_CONSUMER_ID, help='Consumer ID')
    parser.add_argument('--brokers', nargs='+', required=True,
                        help='Broker addresses in format host:port (e.g., localhost:9092 localhost:9093)')
    parser.add_argument('--redis-host', default=Config.REDIS_HOST, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=Config.REDIS_PORT, help='Redis port')
    parser.add_argument('--continuous', action='store_true', help='Continuous consumption mode')
    parser.add_argument('--fetch-all', action='store_true', help='Fetch all messages and exit')

    args = parser.parse_args()

    # Parse broker addresses
    broker_addresses = []
    for broker in args.brokers:
        try:
            host, port = broker.split(':')
            broker_addresses.append((host, int(port)))
        except:
            print(f"Invalid broker address format: {broker}")
            return

    # Create consumer
    consumer = Consumer(args.consumer_id, broker_addresses, args.redis_host, args.redis_port)

    try:
        if args.continuous:
            # Continuous consumption mode
            consumer.consume_messages(continuous=True)

        elif args.fetch_all:
            # Fetch all messages and exit
            messages = consumer.get_all_messages()
            for msg in messages:
                print(f"[Offset {msg['offset']}] {msg['data']}")

        else:
            # Interactive mode
            interactive_mode(consumer)

    finally:
        consumer.close()


if __name__ == '__main__':
    main()
