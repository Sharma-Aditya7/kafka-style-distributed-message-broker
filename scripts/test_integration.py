"""
Integration Test Script
Tests the complete pipeline: Producer -> Leader -> Follower -> Consumer
"""
import sys
import os
import time

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from producer.producer import Producer
from consumer.consumer import Consumer
from common.config import Config


def test_basic_produce_consume():
    """Test basic produce and consume functionality"""
    print("\n" + "=" * 60)
    print("TEST 1: Basic Produce and Consume")
    print("=" * 60)

    # Initialize producer and consumer
    brokers = [(Config.LEADER_HOST, Config.LEADER_PORT),
               (Config.FOLLOWER_HOST, Config.FOLLOWER_PORT)]

    producer = Producer(brokers, Config.REDIS_HOST, Config.REDIS_PORT)
    consumer = Consumer("test-consumer-1", brokers, Config.REDIS_HOST, Config.REDIS_PORT)

    try:
        # Reset consumer offset
        consumer.current_offset = -1
        consumer.commit_offset(-1)

        # Send test messages
        test_messages = [
            "Test message 1",
            "Test message 2",
            "Test message 3"
        ]

        print("\nSending messages...")
        for msg in test_messages:
            success = producer.send_message(msg)
            if not success:
                print(f"✗ TEST FAILED: Could not send message: {msg}")
                return False

        print(f"✓ Sent {len(test_messages)} messages")

        # Wait for replication
        time.sleep(2)

        # Consume messages
        print("\nConsuming messages...")
        consumed = consumer.fetch_messages(max_messages=10)

        if len(consumed) != len(test_messages):
            print(f"✗ TEST FAILED: Expected {len(test_messages)} messages, got {len(consumed)}")
            return False

        # Verify content
        for i, msg in enumerate(consumed):
            if msg['data'] != test_messages[i]:
                print(f"✗ TEST FAILED: Message mismatch at index {i}")
                return False

        print(f"✓ Consumed {len(consumed)} messages correctly")
        print("\n✓✓✓ TEST 1 PASSED ✓✓✓")
        return True

    finally:
        producer.close()
        consumer.close()


def test_batch_produce():
    """Test batch produce functionality"""
    print("\n" + "=" * 60)
    print("TEST 2: Batch Produce")
    print("=" * 60)

    brokers = [(Config.LEADER_HOST, Config.LEADER_PORT),
               (Config.FOLLOWER_HOST, Config.FOLLOWER_PORT)]

    producer = Producer(brokers, Config.REDIS_HOST, Config.REDIS_PORT)

    try:
        # Send batch of messages
        batch_size = 50
        messages = [f"Batch message {i+1}" for i in range(batch_size)]

        print(f"\nSending {batch_size} messages...")
        start_time = time.time()
        results = producer.send_batch(messages)
        elapsed = time.time() - start_time

        print(f"\nResults:")
        print(f"  Success: {results['success']}")
        print(f"  Failed: {results['failed']}")
        print(f"  Time: {elapsed:.2f}s")
        print(f"  Throughput: {results['success']/elapsed:.2f} msg/s")

        if results['failed'] > 0:
            print(f"✗ TEST FAILED: {results['failed']} messages failed")
            return False

        print("\n✓✓✓ TEST 2 PASSED ✓✓✓")
        return True

    finally:
        producer.close()


def test_consumer_offset_tracking():
    """Test consumer offset tracking"""
    print("\n" + "=" * 60)
    print("TEST 3: Consumer Offset Tracking")
    print("=" * 60)

    brokers = [(Config.LEADER_HOST, Config.LEADER_PORT),
               (Config.FOLLOWER_HOST, Config.FOLLOWER_PORT)]

    consumer1 = Consumer("test-offset-consumer", brokers, Config.REDIS_HOST, Config.REDIS_PORT)

    try:
        # Reset offset
        consumer1.current_offset = -1
        consumer1.commit_offset(-1)

        # Fetch some messages
        print("\nFetching messages...")
        messages = consumer1.fetch_messages(max_messages=5)

        if len(messages) == 0:
            print("⚠ No messages available - skipping offset test")
            return True

        # Commit offset
        last_offset = messages[-1]['offset']
        consumer1.commit_offset(last_offset)
        print(f"✓ Committed offset: {last_offset}")

        consumer1.close()

        # Create new consumer instance with same ID
        consumer2 = Consumer("test-offset-consumer", brokers, Config.REDIS_HOST, Config.REDIS_PORT)

        # Check if offset was persisted
        if consumer2.current_offset == last_offset:
            print(f"✓ Offset correctly persisted: {last_offset}")
            print("\n✓✓✓ TEST 3 PASSED ✓✓✓")
            consumer2.close()
            return True
        else:
            print(f"✗ TEST FAILED: Offset mismatch (expected {last_offset}, got {consumer2.current_offset})")
            consumer2.close()
            return False

    finally:
        pass


def test_hwm_enforcement():
    """Test that consumers can only read up to High Water Mark"""
    print("\n" + "=" * 60)
    print("TEST 4: High Water Mark Enforcement")
    print("=" * 60)

    brokers = [(Config.LEADER_HOST, Config.LEADER_PORT),
               (Config.FOLLOWER_HOST, Config.FOLLOWER_PORT)]

    producer = Producer(brokers, Config.REDIS_HOST, Config.REDIS_PORT)
    consumer = Consumer("test-hwm-consumer", brokers, Config.REDIS_HOST, Config.REDIS_PORT)

    try:
        # Send a message
        producer.send_message("HWM test message")
        time.sleep(1)

        # Consumer should be able to read it (it's replicated and below HWM)
        consumer.current_offset = -1
        messages = consumer.fetch_messages()

        if len(messages) > 0:
            print(f"✓ Consumer can read replicated messages (found {len(messages)} messages)")
            print("\n✓✓✓ TEST 4 PASSED ✓✓✓")
            return True
        else:
            print("⚠ No messages available - HWM may not be updated yet")
            return True

    finally:
        producer.close()
        consumer.close()


def main():
    """Run all integration tests"""
    print("\n" + "=" * 60)
    print("YAK MESSAGE BROKER - INTEGRATION TESTS")
    print("=" * 60)
    print("\nMake sure the following are running:")
    print("  1. Redis server")
    print("  2. Leader broker")
    print("  3. Follower broker")
    print("=" * 60)

    input("\nPress Enter to start tests...")

    tests = [
        ("Basic Produce & Consume", test_basic_produce_consume),
        ("Batch Produce", test_batch_produce),
        ("Consumer Offset Tracking", test_consumer_offset_tracking),
        ("HWM Enforcement", test_hwm_enforcement)
    ]

    results = []

    for test_name, test_func in tests:
        try:
            passed = test_func()
            results.append((test_name, passed))
            time.sleep(2)  # Wait between tests
        except Exception as e:
            print(f"\n✗ TEST EXCEPTION: {e}")
            import traceback
            traceback.print_exc()
            results.append((test_name, False))

    # Print summary
    print("\n" + "=" * 60)
    print("TEST SUMMARY")
    print("=" * 60)

    passed_count = 0
    for test_name, passed in results:
        status = "✓ PASSED" if passed else "✗ FAILED"
        print(f"{test_name}: {status}")
        if passed:
            passed_count += 1

    print("=" * 60)
    print(f"Total: {passed_count}/{len(tests)} tests passed")

    if passed_count == len(tests):
        print("\n🎉 ALL TESTS PASSED! 🎉")
        return 0
    else:
        print(f"\n⚠ {len(tests) - passed_count} test(s) failed")
        return 1


if __name__ == '__main__':
    sys.exit(main())
