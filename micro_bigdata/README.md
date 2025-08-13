# Big Data Micro-Coaching

This folder contains daily micro-coaching exercises related to Big Data technologies and their applications in health tech.  
Each entry includes:
- Code for a specific big data concept
- A health tech use case
- Ideas for next steps and scaling

---

## Day 1 — Intro to Spark for Health Data
**File:** `spark_sql_intro.py`  
**Goal:** Practice reading large datasets and performing aggregations using PySpark to simulate healthcare analytics.

**What I Did:**
- Created a `SparkSession`
- Loaded `patients.csv`
- Grouped by gender to calculate average heart rate
- Printed summary statistics

**Health Tech Relevance:**
- Wearable companies (Oura, Fitbit) and health AI firms (Tempus) use Spark to process millions of daily sensor readings.
- Spark is critical when data is **too large for pandas** or needs **distributed computing**.

**Next Steps:**
- Integrate Spark with streaming data (e.g., Kafka)
- Deploy Spark jobs to cloud clusters (AWS EMR, Databricks)

---

## Day 2 — Kafka Producer Simulation for Real-Time Heart Rate Data
**File:** `kafka_stream_sim.py`  
**Goal:** Simulate real-time heart rate data streaming using a Kafka producer.

**What I Did:**
- Wrote a Kafka producer in Python that sends heart rate readings to a `heart_rate_topic` every 2 seconds.
- Used random values to mimic incoming data from a wearable or hospital monitor.

**Health Tech Relevance:**
- Hospitals and wearable companies use Kafka for real-time data ingestion from IoT devices.
- In ICU monitoring, Kafka streams vitals to downstream analytics systems to trigger alerts instantly.

**Next Steps:**
- Add a Kafka consumer to read and process this data
- Integrate with Spark Structured Streaming for near-real-time analytics
- Deploy Kafka to a local cluster or cloud-managed service for testing
