"""
Producer Client Implementation
Sends messages to the leader broker with automatic failover
"""
import socket
import time
import uuid
import sys
import os

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.protocol import (Message, MessageType, send_message, receive_message,
                              create_produce_message)
from common.redis_client import RedisMetadataStore
from common.config import Config


class Producer:
    """Producer client that sends messages to the broker"""

    def __init__(self, broker_addresses: list, redis_host: str, redis_port: int):
        """
        Initialize producer
        broker_addresses: list of (host, port) tuples for all known brokers
        """
        self.broker_addresses = broker_addresses
        self.redis_store = RedisMetadataStore(redis_host, redis_port)

        self.current_leader = None
        self.socket = None

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

    def send_message(self, data: any) -> bool:
        """
        Send a message to the broker
        Returns True if successfully sent and acknowledged, False otherwise
        """
        message_id = str(uuid.uuid4())
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

                # Create and send PRODUCE message
                produce_msg = create_produce_message(data, message_id)
                send_message(self.socket, produce_msg)
                print(f"Sent message: {data}")

                # Wait for ACK
                response = receive_message(self.socket, timeout=Config.SOCKET_TIMEOUT)

                if not response:
                    print("No response from broker")
                    self.socket = None
                    self.current_leader = None
                    retry_count += 1
                    time.sleep(Config.RECONNECT_DELAY)
                    continue

                if response.type == MessageType.ACK:
                    if response.data and response.data.get('success'):
                        print(f"✓ Message acknowledged at offset {response.offset}")
                        return True
                    else:
                        error = response.data.get('error', 'Unknown error')
                        print(f"✗ Message rejected: {error}")
                        return False

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
                        return False

                else:
                    print(f"Unexpected response type: {response.type}")
                    retry_count += 1
                    time.sleep(Config.RECONNECT_DELAY)

            except Exception as e:
                print(f"Error sending message: {e}")
                self.socket = None
                self.current_leader = None
                retry_count += 1
                time.sleep(Config.RECONNECT_DELAY)

        print(f"✗ Failed to send message after {max_retries} retries")
        return False

    def send_batch(self, messages: list) -> dict:
        """
        Send multiple messages
        Returns dict with success count and failure count
        """
        results = {'success': 0, 'failed': 0}

        for msg in messages:
            if self.send_message(msg):
                results['success'] += 1
            else:
                results['failed'] += 1

        return results

    def close(self):
        """Close connection to broker"""
        if self.socket:
            try:
                self.socket.close()
            except:
                pass
        self.redis_store.close()


def interactive_mode(producer: Producer):
    """Interactive mode for sending messages"""
    print("\n" + "=" * 60)
    print("Producer Interactive Mode")
    print("=" * 60)
    print("Commands:")
    print("  - Type a message and press Enter to send")
    print("  - Type 'quit' or 'exit' to stop")
    print("  - Type 'batch <N>' to send N test messages")
    print("=" * 60 + "\n")

    while True:
        try:
            user_input = input("Message > ").strip()

            if user_input.lower() in ['quit', 'exit']:
                print("Exiting producer...")
                break

            elif user_input.lower().startswith('batch'):
                try:
                    parts = user_input.split()
                    count = int(parts[1]) if len(parts) > 1 else 10

                    print(f"Sending {count} test messages...")
                    messages = [f"Test message {i+1}" for i in range(count)]

                    start_time = time.time()
                    results = producer.send_batch(messages)
                    elapsed = time.time() - start_time

                    print(f"\n✓ Batch complete:")
                    print(f"  Success: {results['success']}")
                    print(f"  Failed: {results['failed']}")
                    print(f"  Time: {elapsed:.2f}s")
                    print(f"  Throughput: {results['success']/elapsed:.2f} msg/s\n")

                except ValueError:
                    print("Invalid batch count")

            elif user_input:
                producer.send_message(user_input)

        except KeyboardInterrupt:
            print("\nExiting producer...")
            break
        except Exception as e:
            print(f"Error: {e}")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Producer Client')
    parser.add_argument('--brokers', nargs='+', required=True,
                        help='Broker addresses in format host:port (e.g., localhost:9092 localhost:9093)')
    parser.add_argument('--redis-host', default=Config.REDIS_HOST, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=Config.REDIS_PORT, help='Redis port')
    parser.add_argument('--message', help='Single message to send (non-interactive mode)')
    parser.add_argument('--batch', type=int, help='Send N test messages and exit')

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

    # Create producer
    producer = Producer(broker_addresses, args.redis_host, args.redis_port)

    try:
        if args.message:
            # Send single message and exit
            success = producer.send_message(args.message)
            sys.exit(0 if success else 1)

        elif args.batch:
            # Send batch of messages and exit
            messages = [f"Test message {i+1}" for i in range(args.batch)]
            results = producer.send_batch(messages)
            print(f"\nResults: {results['success']} success, {results['failed']} failed")
            sys.exit(0 if results['failed'] == 0 else 1)

        else:
            # Interactive mode
            interactive_mode(producer)

    finally:
        producer.close()


if __name__ == '__main__':
    main()
