"""
Flight Data Consumer
Processes airline flight delay data from the broker
Performs real-time analytics on flight delays
"""
import json
import time
import sys
import os
from collections import defaultdict
from typing import Dict, List

# Add parent directory to path for imports
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumer.consumer import Consumer
from common.config import Config


class FlightDataConsumer:
    """Consumer that processes flight delay data and performs analytics"""

    def __init__(self, consumer_id: str, broker_addresses: list, redis_host: str, redis_port: int):
        self.consumer = Consumer(consumer_id, broker_addresses, redis_host, redis_port)
        self.statistics = {
            'total_flights': 0,
            'delayed_flights': 0,
            'total_delay_minutes': 0,
            'airlines': defaultdict(int),
            'routes': defaultdict(int),
            'delays_by_airline': defaultdict(list),
            'delays_by_route': defaultdict(list)
        }

    def process_flight_record(self, message: dict):
        """Process a single flight record"""
        try:
            # Parse JSON data
            flight_data = json.loads(message['data'])

            # Update statistics
            self.statistics['total_flights'] += 1
            self.statistics['airlines'][flight_data['airline']] += 1

            route = f"{flight_data['origin']}->{flight_data['destination']}"
            self.statistics['routes'][route] += 1

            # Track delays
            dep_delay = flight_data.get('departure_delay', 0)
            arr_delay = flight_data.get('arrival_delay', 0)

            if dep_delay > 0 or arr_delay > 0:
                self.statistics['delayed_flights'] += 1
                total_delay = dep_delay + arr_delay
                self.statistics['total_delay_minutes'] += total_delay

                self.statistics['delays_by_airline'][flight_data['airline']].append(total_delay)
                self.statistics['delays_by_route'][route].append(total_delay)

            return True

        except json.JSONDecodeError as e:
            print(f"❌ Error parsing JSON: {e}")
            return False
        except Exception as e:
            print(f"❌ Error processing record: {e}")
            return False

    def print_statistics(self):
        """Print analytics statistics"""
        print(f"\n{'='*70}")
        print(f"FLIGHT DELAY ANALYTICS")
        print(f"{'='*70}")

        total = self.statistics['total_flights']
        delayed = self.statistics['delayed_flights']
        delay_rate = (delayed / total * 100) if total > 0 else 0

        print(f"\n📊 Overall Statistics:")
        print(f"  Total Flights Processed: {total:,}")
        print(f"  Delayed Flights: {delayed:,}")
        print(f"  On-Time Flights: {total - delayed:,}")
        print(f"  Delay Rate: {delay_rate:.2f}%")
        print(f"  Total Delay Minutes: {self.statistics['total_delay_minutes']:,}")

        if delayed > 0:
            avg_delay = self.statistics['total_delay_minutes'] / delayed
            print(f"  Average Delay: {avg_delay:.2f} minutes")

        # Top airlines by flight count
        print(f"\n✈️  Top Airlines by Flight Count:")
        sorted_airlines = sorted(self.statistics['airlines'].items(),
                                 key=lambda x: x[1], reverse=True)[:10]
        for airline, count in sorted_airlines:
            percentage = (count / total * 100) if total > 0 else 0
            print(f"  {airline}: {count:,} flights ({percentage:.1f}%)")

        # Top routes
        print(f"\n🛫 Top Routes:")
        sorted_routes = sorted(self.statistics['routes'].items(),
                               key=lambda x: x[1], reverse=True)[:10]
        for route, count in sorted_routes:
            print(f"  {route}: {count:,} flights")

        # Airlines with worst delays
        if self.statistics['delays_by_airline']:
            print(f"\n⏰ Airlines with Highest Average Delays:")
            airline_avg_delays = {}
            for airline, delays in self.statistics['delays_by_airline'].items():
                if len(delays) >= 5:  # Only consider airlines with at least 5 delays
                    airline_avg_delays[airline] = sum(delays) / len(delays)

            sorted_delay_airlines = sorted(airline_avg_delays.items(),
                                           key=lambda x: x[1], reverse=True)[:10]
            for airline, avg_delay in sorted_delay_airlines:
                delay_count = len(self.statistics['delays_by_airline'][airline])
                print(f"  {airline}: {avg_delay:.2f} min avg ({delay_count} delayed flights)")

        # Routes with worst delays
        if self.statistics['delays_by_route']:
            print(f"\n🛣️  Routes with Highest Average Delays:")
            route_avg_delays = {}
            for route, delays in self.statistics['delays_by_route'].items():
                if len(delays) >= 3:  # Only consider routes with at least 3 delays
                    route_avg_delays[route] = sum(delays) / len(delays)

            sorted_delay_routes = sorted(route_avg_delays.items(),
                                         key=lambda x: x[1], reverse=True)[:10]
            for route, avg_delay in sorted_delay_routes:
                delay_count = len(self.statistics['delays_by_route'][route])
                print(f"  {route}: {avg_delay:.2f} min avg ({delay_count} delayed flights)")

        print(f"\n{'='*70}\n")

    def consume_and_analyze(self, continuous: bool = True, poll_interval: int = 2):
        """Consume messages and perform real-time analytics"""
        print(f"\n{'='*70}")
        print(f"Starting Flight Data Consumer")
        print(f"Consumer ID: {self.consumer.consumer_id}")
        print(f"Mode: {'Continuous' if continuous else 'Single Fetch'}")
        print(f"{'='*70}\n")

        processed = 0
        last_stats_time = time.time()
        stats_interval = 10  # Print stats every 10 seconds

        try:
            while True:
                messages = self.consumer.fetch_messages(max_messages=100)

                if messages:
                    for msg in messages:
                        if self.process_flight_record(msg):
                            processed += 1

                            # Print progress every 100 records
                            if processed % 100 == 0:
                                print(f"✓ Processed {processed} flight records...")

                        # Commit offset after processing
                        self.consumer.commit_offset(msg['offset'])

                    # Print periodic statistics
                    if time.time() - last_stats_time >= stats_interval:
                        self.print_statistics()
                        last_stats_time = time.time()

                else:
                    if not continuous:
                        break
                    print("⏳ No new messages, waiting...")

                if not continuous:
                    break

                time.sleep(poll_interval)

        except KeyboardInterrupt:
            print("\n\n⏹️  Stopping consumer...")
        finally:
            # Print final statistics
            print(f"\n📈 Final Statistics:")
            self.print_statistics()
            self.consumer.close()

    def batch_analyze(self):
        """Fetch all messages and perform batch analytics"""
        print(f"\n{'='*70}")
        print(f"Batch Analytics Mode")
        print(f"Fetching all available flight data...")
        print(f"{'='*70}\n")

        messages = self.consumer.get_all_messages()

        print(f"✓ Fetched {len(messages)} flight records\n")
        print(f"Processing analytics...\n")

        for msg in messages:
            self.process_flight_record(msg)

        self.print_statistics()
        self.consumer.close()


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Flight Data Consumer & Analytics')
    parser.add_argument('--consumer-id', default='flight-analytics-consumer', help='Consumer ID')
    parser.add_argument('--brokers', nargs='+', required=True,
                        help='Broker addresses (e.g., localhost:9092 localhost:9093)')
    parser.add_argument('--redis-host', default=Config.REDIS_HOST, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=Config.REDIS_PORT, help='Redis port')
    parser.add_argument('--continuous', action='store_true', help='Continuous consumption mode')
    parser.add_argument('--batch', action='store_true', help='Batch analytics mode (fetch all)')

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
    flight_consumer = FlightDataConsumer(
        args.consumer_id,
        broker_addresses,
        args.redis_host,
        args.redis_port
    )

    print(f"\n📊 Flight Data Analytics Consumer Starting...\n")

    if args.batch:
        # Batch mode - fetch all and analyze
        flight_consumer.batch_analyze()
    else:
        # Real-time mode
        flight_consumer.consume_and_analyze(continuous=args.continuous)


if __name__ == '__main__':
    main()
