"""
Spark Streaming Job for Real-Time Flight Delay Analysis
Connects to YAK broker and processes flight delay data in real-time
"""
import sys
import os
import json
import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from consumer.consumer import Consumer
from common.config import Config


class SparkFlightDelayAnalyzer:
    """Spark-based flight delay analyzer"""

    def __init__(self, app_name="FlightDelayAnalytics"):
        """Initialize Spark session"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.sql.streaming.schemaInference", "true") \
            .config("spark.sql.shuffle.partitions", "4") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

        print(f"✓ Spark Session initialized: {app_name}")
        print(f"  Spark Version: {self.spark.version}")
        print(f"  Master: {self.spark.sparkContext.master}\n")

    def define_schema(self):
        """Define schema for flight data"""
        return StructType([
            StructField("airline", StringType(), True),
            StructField("origin", StringType(), True),
            StructField("destination", StringType(), True),
            StructField("departure_time", StringType(), True),
            StructField("departure_delay", IntegerType(), True),
            StructField("arrival_delay", IntegerType(), True),
            StructField("flight_time", IntegerType(), True),
            StructField("distance", IntegerType(), True),
            StructField("timestamp", DoubleType(), True)
        ])

    def fetch_batch_from_broker(self, broker_addresses, redis_host, redis_port, consumer_id="spark-consumer"):
        """
        Fetch flight data from YAK broker
        Returns a Spark DataFrame
        """
        print(f"Connecting to YAK broker...")
        consumer = Consumer(consumer_id, broker_addresses, redis_host, redis_port)

        try:
            # Fetch all available messages
            messages = consumer.get_all_messages()
            print(f"✓ Fetched {len(messages)} records from broker\n")

            # Extract flight data
            flight_records = []
            for msg in messages:
                try:
                    flight_data = json.loads(msg['data'])
                    flight_records.append(flight_data)
                except json.JSONDecodeError:
                    continue

            consumer.close()

            # Create DataFrame
            if flight_records:
                df = self.spark.createDataFrame(flight_records, schema=self.define_schema())
                return df
            else:
                print("No flight records found")
                return None

        except Exception as e:
            print(f"Error fetching from broker: {e}")
            consumer.close()
            return None

    def analyze_delays(self, df):
        """Perform comprehensive delay analysis"""
        if df is None or df.count() == 0:
            print("No data to analyze")
            return

        print(f"\n{'='*80}")
        print(f"SPARK FLIGHT DELAY ANALYSIS")
        print(f"{'='*80}\n")

        # Register as temp view for SQL queries
        df.createOrReplaceTempView("flights")

        # 1. Overall Statistics
        print("📊 Overall Statistics:")
        print("-" * 80)

        total_flights = df.count()
        delayed_flights = df.filter((col("departure_delay") > 0) | (col("arrival_delay") > 0)).count()
        delay_rate = (delayed_flights / total_flights * 100) if total_flights > 0 else 0

        print(f"Total Flights: {total_flights:,}")
        print(f"Delayed Flights: {delayed_flights:,}")
        print(f"On-Time Flights: {total_flights - delayed_flights:,}")
        print(f"Delay Rate: {delay_rate:.2f}%\n")

        # 2. Delay statistics
        delay_stats = df.select(
            avg(col("departure_delay")).alias("avg_dep_delay"),
            avg(col("arrival_delay")).alias("avg_arr_delay"),
            max(col("departure_delay")).alias("max_dep_delay"),
            max(col("arrival_delay")).alias("max_arr_delay")
        ).collect()[0]

        print(f"Average Departure Delay: {delay_stats['avg_dep_delay']:.2f} minutes")
        print(f"Average Arrival Delay: {delay_stats['avg_arr_delay']:.2f} minutes")
        print(f"Max Departure Delay: {delay_stats['max_dep_delay']} minutes")
        print(f"Max Arrival Delay: {delay_stats['max_arr_delay']} minutes\n")

        # 3. Top Airlines by Flight Count
        print("✈️  Top 10 Airlines by Flight Count:")
        print("-" * 80)
        airline_counts = df.groupBy("airline") \
            .agg(count("*").alias("flight_count")) \
            .orderBy(desc("flight_count")) \
            .limit(10)

        airline_counts.show(truncate=False)

        # 4. Airlines with Worst Delays
        print("⏰ Top 10 Airlines by Average Delay:")
        print("-" * 80)
        airline_delays = df.filter((col("departure_delay") > 0) | (col("arrival_delay") > 0)) \
            .withColumn("total_delay", col("departure_delay") + col("arrival_delay")) \
            .groupBy("airline") \
            .agg(
                avg("total_delay").alias("avg_delay"),
                count("*").alias("delayed_flights")
            ) \
            .filter(col("delayed_flights") >= 5) \
            .orderBy(desc("avg_delay")) \
            .limit(10)

        airline_delays.show(truncate=False)

        # 5. Busiest Routes
        print("🛫 Top 10 Busiest Routes:")
        print("-" * 80)
        df_with_route = df.withColumn("route", concat(col("origin"), lit("->"), col("destination")))
        route_counts = df_with_route.groupBy("route") \
            .agg(count("*").alias("flight_count")) \
            .orderBy(desc("flight_count")) \
            .limit(10)

        route_counts.show(truncate=False)

        # 6. Routes with Worst Delays
        print("🛣️  Top 10 Routes by Average Delay:")
        print("-" * 80)
        route_delays = df_with_route.filter((col("departure_delay") > 0) | (col("arrival_delay") > 0)) \
            .withColumn("total_delay", col("departure_delay") + col("arrival_delay")) \
            .groupBy("route") \
            .agg(
                avg("total_delay").alias("avg_delay"),
                count("*").alias("delayed_flights")
            ) \
            .filter(col("delayed_flights") >= 3) \
            .orderBy(desc("avg_delay")) \
            .limit(10)

        route_delays.show(truncate=False)

        # 7. Delays by Time of Day
        print("🕐 Delays by Departure Time (Hour):")
        print("-" * 80)

        # Extract hour from departure time (format: HHMM)
        df_with_hour = df.withColumn(
            "departure_hour",
            when(length(col("departure_time")) == 4,
                 substring(col("departure_time"), 1, 2).cast("int"))
            .when(length(col("departure_time")) == 3,
                  substring(col("departure_time"), 1, 1).cast("int"))
            .otherwise(0)
        )

        hourly_delays = df_with_hour.filter((col("departure_delay") > 0) | (col("arrival_delay") > 0)) \
            .withColumn("total_delay", col("departure_delay") + col("arrival_delay")) \
            .groupBy("departure_hour") \
            .agg(
                avg("total_delay").alias("avg_delay"),
                count("*").alias("delayed_flights")
            ) \
            .orderBy("departure_hour")

        hourly_delays.show(24, truncate=False)

        # 8. Distance vs Delay Analysis
        print("📏 Distance vs Delay Analysis:")
        print("-" * 80)

        distance_delays = df.filter((col("departure_delay") > 0) | (col("arrival_delay") > 0)) \
            .withColumn("total_delay", col("departure_delay") + col("arrival_delay")) \
            .withColumn("distance_category",
                        when(col("distance") < 500, "Short (<500 mi)")
                        .when((col("distance") >= 500) & (col("distance") < 1000), "Medium (500-1000 mi)")
                        .otherwise("Long (>1000 mi)")) \
            .groupBy("distance_category") \
            .agg(
                avg("total_delay").alias("avg_delay"),
                count("*").alias("delayed_flights"),
                avg("distance").alias("avg_distance")
            ) \
            .orderBy("avg_distance")

        distance_delays.show(truncate=False)

        print(f"\n{'='*80}\n")

    def save_results(self, df, output_path="output/flight_analysis"):
        """Save analysis results to disk"""
        if df is None:
            return

        print(f"Saving results to {output_path}...")

        try:
            # Save as parquet (columnar format, good for analytics)
            df.write.mode("overwrite").parquet(f"{output_path}/parquet")
            print(f"✓ Saved as Parquet: {output_path}/parquet")

            # Save as JSON for easy viewing
            df.write.mode("overwrite").json(f"{output_path}/json")
            print(f"✓ Saved as JSON: {output_path}/json")

            # Save summary statistics as CSV
            summary = df.describe()
            summary.write.mode("overwrite").csv(f"{output_path}/summary_csv", header=True)
            print(f"✓ Saved summary: {output_path}/summary_csv\n")

        except Exception as e:
            print(f"Error saving results: {e}")

    def stop(self):
        """Stop Spark session"""
        self.spark.stop()
        print("✓ Spark session stopped")


def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(description='Spark Flight Delay Analytics')
    parser.add_argument('--brokers', nargs='+', required=True,
                        help='Broker addresses (e.g., localhost:9092 localhost:9093)')
    parser.add_argument('--redis-host', default=Config.REDIS_HOST, help='Redis host')
    parser.add_argument('--redis-port', type=int, default=Config.REDIS_PORT, help='Redis port')
    parser.add_argument('--consumer-id', default='spark-analytics-consumer', help='Consumer ID')
    parser.add_argument('--output', default='output/flight_analysis', help='Output directory')
    parser.add_argument('--save', action='store_true', help='Save results to disk')

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

    print(f"\n{'='*80}")
    print(f"SPARK FLIGHT DELAY ANALYTICS")
    print(f"{'='*80}\n")

    # Create Spark analyzer
    analyzer = SparkFlightDelayAnalyzer()

    try:
        # Fetch data from YAK broker
        df = analyzer.fetch_batch_from_broker(
            broker_addresses,
            args.redis_host,
            args.redis_port,
            args.consumer_id
        )

        if df:
            # Perform analysis
            analyzer.analyze_delays(df)

            # Save results if requested
            if args.save:
                analyzer.save_results(df, args.output)

    finally:
        analyzer.stop()


if __name__ == '__main__':
    main()
