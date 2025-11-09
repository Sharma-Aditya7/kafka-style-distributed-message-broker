"""
Flight Data Producer
Streams airline flight delay data from CSV to the broker
"""
import csv
import time
import sys
import os
import json

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.producer import Producer
from common.config import Config


class FlightDataProducer:
    """Producer that streams flight delay data from CSV"""

    def __init__(self, csv_file_path: str, broker_addresses: list, redis_host: str, redis_port: int):
        self.csv_file_path = csv_file_path
        self.producer = Producer(broker_addresses, redis_host, redis_port)
        self.total_sent = 0
        self.total_failed = 0

    def stream_flight_data(self, delay_ms: int = 100, max_records: int = 200):
        """
        Stream flight data from CSV file

        Args:
            delay_ms: Milliseconds to wait between sending records (simulates real-time streaming)
            max_records: Maximum number of records to send (None = all)
        """
        print(f"\n{'='*60}")
        print(f"Starting Flight Data Stream")
        print(f"{'='*60}")
        print(f"CSV File: {self.csv_file_path}")
        print(f"Streaming Delay: {delay_ms}ms per record")
        print(f"Max Records: {max_records if max_records else 'All'}")
        print(f"{'='*60}\n")

        try:
            with open(self.csv_file_path, 'r', encoding='utf-8-sig') as csvfile:
                reader = csv.DictReader(csvfile)

                # Verify CSV headers
                expected_fields = ['Marketing_Airline_Network', 'Origin', 'Dest', 'CRSDepTime',
                                   'DepDelayMinutes', 'ArrDelayMinutes', 'CRSElapsedTime', 'Distance']

                if not all(field in reader.fieldnames for field in expected_fields):
                    print(f"❌ Error: CSV missing required fields")
                    print(f"Expected: {expected_fields}")
                    print(f"Found: {reader.fieldnames}")
                    return

                print(f"✓ CSV validated - found all required fields\n")

                record_count = 0
                start_time = time.time()

                for row in reader:
                    if max_records and record_count >= max_records:
                        break

                    # Create flight record
                    flight_record = {
                        'airline': row['Marketing_Airline_Network'],
                        'origin': row['Origin'],
                        'destination': row['Dest'],
                        'departure_time': row['CRSDepTime'],
                        'departure_delay': int(row['DepDelayMinutes']) if row['DepDelayMinutes'] else 0,
                        'arrival_delay': int(row['ArrDelayMinutes']) if row['ArrDelayMinutes'] else 0,
                        'flight_time': int(row['CRSElapsedTime']) if row['CRSElapsedTime'] else 0,
                        'distance': int(row['Distance']) if row['Distance'] else 0,
                        'timestamp': time.time()
                    }

                    # Convert to JSON string
                    message_data = json.dumps(flight_record)

                    # Send to broker
                    success = self.producer.send_message(message_data)

                    if success:
                        self.total_sent += 1
                        record_count += 1

                        # Print progress every 100 records
                        if record_count % 100 == 0:
                            elapsed = time.time() - start_time
                            rate = record_count / elapsed if elapsed > 0 else 0
                            print(f"✓ Sent {record_count} records ({rate:.1f} rec/sec) | "
                                  f"Latest: {flight_record['airline']} {flight_record['origin']}->{flight_record['destination']}")
                    else:
                        self.total_failed += 1
                        print(f"✗ Failed to send record {record_count}")

                    # Simulate real-time streaming delay
                    time.sleep(delay_ms / 1000.0)

                # Final statistics
                elapsed = time.time() - start_time
                print(f"\n{'='*60}")
                print(f"Streaming Complete")
                print(f"{'='*60}")
                print(f"Total Records Sent: {self.total_sent}")
                print(f"Total Failed: {self.total_failed}")
                print(f"Total Time: {elapsed:.2f} seconds")
                print(f"Average Rate: {self.total_sent/elapsed:.2f} records/sec")
                print(f"{'='*60}\n")

        except FileNotFoundError:
            print(f"❌ Error: CSV file not found: {self.csv_file_path}")
        except Exception as e:
            print(f"❌ Error streaming data: {e}")
            import traceback
            traceback.print_exc()
        finally:
            self.producer.close()

    def send_single_flight(self, flight_data: dict):
        """Send a single flight record"""
        message_data = json.dumps(flight_data)
        return self.producer.send_message(message_data)


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Flight Data Producer')
    parser.add_argument('--csv', default='data/FlightDelay2.csv', help='Path to flight data CSV')
    parser.add_argument('--brokers', nargs='+', required=True,
                        help='Broker addresses (e.g., localhost:9092 localhost:9093)')
    parser.add_argument('--redis-host', default=Config.REDIS_HOST, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=Config.REDIS_PORT, help='Redis port')
    parser.add_argument('--delay', type=int, default=100,
                        help='Delay between records in milliseconds (default: 100)')
    parser.add_argument('--max-records', type=int, help='Maximum records to send')
    parser.add_argument('--fast', action='store_true', help='Fast mode (no delay)')

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

    # Determine delay
    delay = 0 if args.fast else args.delay

    # Create producer and stream data
    flight_producer = FlightDataProducer(
        args.csv,
        broker_addresses,
        args.redis_host,
        args.redis_port
    )

    print(f"\n🛫 Flight Data Producer Starting...")
    print(f"Mode: {'FAST (no delay)' if args.fast else f'Streaming ({delay}ms delay)'}\n")

    flight_producer.stream_flight_data(delay_ms=delay, max_records=args.max_records)


if __name__ == '__main__':
    main()
