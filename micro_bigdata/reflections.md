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

## Day 6: Troubleshooting Distributed Systems

### 1. Environment Compatibility is Critical
- Java 24 broke PySpark due to `getSubject is not supported`
- Always check compatibility matrices before upgrading dependencies
- Big data tools often lag behind latest Java versions

### 2. Always Have a Fallback Plan
- When Spark failed, we built a Python streaming processor
- Same concepts, different implementation: file watching, micro-batches, JSON parsing
- Don't let tooling failures block learning the core concepts

### 3. Streaming Pipeline Fundamentals
- **Source → Processing → Sink** pattern is universal
- File position tracking simulates Kafka offset management
- Micro-batch processing (3-second intervals) balances latency vs throughput
- Schema validation prevents downstream errors

### 4. Production-Ready Error Handling
- Try-catch around JSON parsing for malformed records
- Graceful shutdown with Ctrl+C handling
- State tracking (file position, processed count)
- Alert logic (HR outside 60-100 bpm range)

### 5. Real-time Data Processing Patterns
- **Structured streaming**: Define schema upfront for type safety
- **Incremental processing**: Only read new data since last checkpoint
- **Stateful operations**: Track what's been processed to avoid duplicates
- **Monitoring**: Processing timestamps, batch sizes, error rates

### 6. Development Strategy
- Start simple, add complexity gradually
- Build working prototypes before production systems
- Test with sample data first, then real streams
- Demonstrate concepts even when ideal tools aren't available

**Key Insight**: The Python version proves you understand streaming fundamentals - tool choice is secondary to concepts!