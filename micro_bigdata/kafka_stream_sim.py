"""
Kafka Streaming Simulation (Upgraded)
- Mode 'kafka': send patient vitals to Kafka topic 'patient_vitals'
- Mode 'file' : append JSON lines to 'heart_rate_stream.jsonl' for Spark fallback

Usage examples:
  python kafka_stream_sim.py                 # defaults to --mode kafka
  python kafka_stream_sim.py --mode file     # writes JSONL for Spark fallback
  python kafka_stream_sim.py --mode kafka --rate 1 --count 100
"""

import argparse
import json
import random
import signal
import sys
import time
from datetime import datetime

# ----------------- CLI -----------------
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--mode", choices=["kafka", "file"], default="kafka",
                   help="Where to send events")
    p.add_argument("--bootstrap", default="localhost:9092",
                   help="Kafka bootstrap servers")
    p.add_argument("--topic", default="patient_vitals",
                   help="Kafka topic")
    p.add_argument("--outfile", default="heart_rate_stream.jsonl",
                   help="JSONL file for --mode file")
    p.add_argument("--rate", type=float, default=2.0,
                   help="Seconds between messages")
    p.add_argument("--count", type=int, default=0,
                   help="Number of messages to send (0 = infinite)")
    return p.parse_args()

# ----------------- Data generator -----------------
def make_event():
    return {
        "patient_id": random.randint(1000, 1010),
        "heart_rate": random.randint(60, 100),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

# ----------------- Kafka producer (optional) -----------------
def build_kafka_producer(bootstrap):
    try:
        from kafka import KafkaProducer  # requires 'kafka-python'
        producer = KafkaProducer(
            bootstrap_servers=bootstrap,
            acks="all",
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            key_serializer=lambda k: str(k).encode("utf-8"),
            retries=10,
            linger_ms=20,                 # small batching for throughput
            enable_idempotence=True       # avoid duplicates on retry
        )
        return producer
    except Exception as e:
        print(f"[WARN] Could not create Kafka producer: {e}")
        return None

# ----------------- Main -----------------
def main():
    args = parse_args()
    print(f"=== Streaming mode: {args.mode} ===")

    # graceful Ctrl+C
    stop = {"flag": False}
    def handle_sigint(_sig, _frm): stop["flag"] = True
    signal.signal(signal.SIGINT, handle_sigint)

    producer = None
    outfh = None

    if args.mode == "kafka":
        producer = build_kafka_producer(args.bootstrap)
        if producer is None:
            print("[INFO] Falling back to file mode because Kafka is unavailable.")
            args.mode = "file"

    if args.mode == "file":
        outfh = open(args.outfile, "a", buffering=1)

    sent = 0
    try:
        while not stop["flag"] and (args.count == 0 or sent < args.count):
            event = make_event()

            if args.mode == "kafka":
                # key by patient_id to preserve per-patient ordering
                fut = producer.send(args.topic, key=event["patient_id"], value=event)
                fut.get(timeout=10)  # confirm send
            else:
                outfh.write(json.dumps(event) + "\n")

            sent += 1
            print("Sent:", event)
            time.sleep(args.rate)

    finally:
        if producer:
            try: producer.flush(5)
            except Exception: pass
            try: producer.close()
            except Exception: pass
        if outfh:
            outfh.close()
        print("Shutting down producer.")

if __name__ == "__main__":
    main()
