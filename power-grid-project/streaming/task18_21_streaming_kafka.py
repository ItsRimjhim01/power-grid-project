"""
Tasks 18–21: Streaming, Kafka, Structured Streaming
Project: Power Grid Analytics Dashboard
Description: Simulate real-time data streaming for power grid monitoring.

Tasks:
  18. Streaming Concepts - Simulate real-time data processing
  19. Kafka Basics       - Producer-Consumer pipeline simulation
  20. Kafka Advanced     - Partitioning and offset management
  21. Structured Streaming - Real-time log processing
"""

import random
import time
import json
import threading
import queue
import os
from datetime import datetime
from collections import defaultdict, deque

print("=" * 60)
print("  Tasks 18–21: Streaming & Kafka - Power Grid Real-Time")
print("=" * 60)

# ============================================================
# TASK 18: Streaming Concepts
# ============================================================
print("\n" + "="*50)
print("  TASK 18: Real-Time Streaming Concepts")
print("="*50)

print("""
  Streaming Architecture for Power Grid:
  =======================================

  [IoT Sensors / Grid Meters]
          ↓ (real-time events every second)
  [Kafka Topic: power-readings]
          ↓ (consumer groups)
  [Spark Structured Streaming / Flink]
          ↓ (windowed aggregations)
  [Real-Time Dashboard / Alerts]
          ↓
  [MySQL / Data Lake]

  Processing Models:
  - Micro-batch (Spark Streaming): collect 5s batches → process
  - Event-time  (Flink/Kafka Streams): process each event as it arrives
  - Windowing   : 5-min tumbling window of avg consumption per region
""")

# ============================================================
# TASK 19: Kafka Basics - Producer-Consumer Pipeline
# ============================================================
print("="*50)
print("  TASK 19: Kafka Producer-Consumer Simulation")
print("="*50)

# Simulated Kafka using Python Queue
kafka_topic = queue.Queue(maxsize=100)
regions = ["North", "South", "East", "West"]

produced_messages = []
consumed_messages = []

def producer(n_messages=15, delay=0.05):
    """Simulate Kafka Producer sending power readings."""
    for i in range(n_messages):
        msg = {
            "offset"     : i,
            "timestamp"  : str(datetime.now()),
            "region"     : random.choice(regions),
            "consumption": round(random.uniform(100, 500), 2),
            "voltage"    : round(random.uniform(220, 240), 1),
        }
        kafka_topic.put(msg)
        produced_messages.append(msg)
        time.sleep(delay)

def consumer(label="Consumer-1", timeout=2):
    """Simulate Kafka Consumer reading and processing messages."""
    while True:
        try:
            msg = kafka_topic.get(timeout=timeout)
            consumed_messages.append(msg)
            kafka_topic.task_done()
        except queue.Empty:
            break

print("\n[Kafka] Starting Producer and Consumer threads...")
t_prod = threading.Thread(target=producer, args=(15, 0.03))
t_cons = threading.Thread(target=consumer)
t_prod.start()
t_prod.join()
t_cons.start()
t_cons.join()

print(f"  Messages Produced : {len(produced_messages)}")
print(f"  Messages Consumed : {len(consumed_messages)}")
print(f"\n  Last 3 consumed messages:")
for msg in consumed_messages[-3:]:
    print(f"    Offset {msg['offset']} | {msg['region']} | {msg['consumption']} kWh")

# ============================================================
# TASK 20: Kafka Advanced - Partitioning & Offset Management
# ============================================================
print("\n" + "="*50)
print("  TASK 20: Kafka Advanced - Partitions & Offsets")
print("="*50)

# Simulate 4 partitions (one per region)
partitions = {r: [] for r in regions}
partition_offsets = {r: 0 for r in regions}
consumer_offsets  = {r: 0 for r in regions}  # tracks what's been consumed

print("\n[Kafka] Publishing 20 messages to partitioned topic...")
for i in range(20):
    region = random.choice(regions)
    msg = {
        "partition": region,
        "offset"   : partition_offsets[region],
        "value"    : {"consumption": round(random.uniform(100,500),2), "ts": str(datetime.now())}
    }
    partitions[region].append(msg)
    partition_offsets[region] += 1

for region, msgs in partitions.items():
    print(f"  Partition [{region}]: {len(msgs)} messages | Latest offset: {partition_offsets[region]-1}")

# Simulate consumer reading with offset management
print("\n[Kafka] Consumer reading from offset 0 for each partition...")
for region in regions:
    start_offset = consumer_offsets[region]
    to_consume   = partitions[region][start_offset:]
    consumer_offsets[region] = len(partitions[region])
    print(f"  [{region}] Read {len(to_consume)} messages | Committed offset: {consumer_offsets[region]}")

print("""
  Key Kafka Concepts:
  ✅ Partitions → parallelize consumption (1 partition per region)
  ✅ Offsets    → track where each consumer left off (at-least-once delivery)
  ✅ Consumer Groups → multiple consumers share partitions
  ✅ Retention  → messages kept for 7 days (replay possible)
""")

# ============================================================
# TASK 21: Structured Streaming - Real-time log processing
# ============================================================
print("="*50)
print("  TASK 21: Structured Streaming - Real-Time Log Analysis")
print("="*50)

os.makedirs("logs", exist_ok=True)

# Simulate streaming micro-batches
WINDOW_SIZE = 5  # 5-record tumbling window
stream_buffer = deque(maxlen=WINDOW_SIZE)
batch_results = []

print("\n[Streaming] Processing power events in 5-record micro-batches...\n")

all_events = produced_messages.copy()
random.shuffle(all_events)

batch_num = 0
for i, event in enumerate(all_events):
    stream_buffer.append(event)

    if len(stream_buffer) == WINDOW_SIZE:
        batch_num += 1
        window_df = list(stream_buffer)

        avg_consumption = round(sum(e["consumption"] for e in window_df) / WINDOW_SIZE, 2)
        max_consumption = max(e["consumption"] for e in window_df)
        alerts = [e for e in window_df if e["consumption"] > 400]

        result = {
            "batch"         : batch_num,
            "window_size"   : WINDOW_SIZE,
            "avg_kwh"       : avg_consumption,
            "max_kwh"       : round(max_consumption, 2),
            "alert_count"   : len(alerts),
        }
        batch_results.append(result)
        stream_buffer.clear()

        print(f"  Batch {batch_num}: avg={avg_consumption} kWh | max={result['max_kwh']} | alerts={result['alert_count']}")

        # Write streaming output to log
        with open("logs/streaming_output.log", "a") as f:
            f.write(json.dumps(result) + "\n")

print(f"\n  Total batches processed: {batch_num}")
print(f"  Total alerts generated : {sum(r['alert_count'] for r in batch_results)}")
print(f"  Streaming log saved    → logs/streaming_output.log")

print("\n✅ Tasks 18–21: Streaming & Kafka Simulation Complete.")
