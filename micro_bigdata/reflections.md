# Code Concepts Learned

## Day 5: Kafka Streaming & Real-time Data Processing

### Code Concepts Learned (Day 5):

Kafka `producer.send()` returns a future → confirmed delivery.

Keyed messages (`key=str(patient_id)`) ensure patient events stay ordered per ID.

Idempotence prevents duplicates if retries happen.

`argparse` makes the script usable like a real CLI tool:

```bash
python kafka_stream_sim.py --mode file --outfile heart_rate_stream.json --count 100
```

Graceful shutdown (Ctrl+C) with `signal.signal(SIGINT, handler)` → essential for real-time systems.