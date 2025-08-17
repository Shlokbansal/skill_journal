"""
Spark Streaming Reader for patient vitals
Reads from JSONL file (heart_rate_stream.jsonl) and processes streaming data
"""

import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. Define schema for patient JSON
schema = StructType() \
    .add("patient_id", IntegerType()) \
    .add("heart_rate", IntegerType()) \
    .add("timestamp", StringType())

# 2. Start Spark session (simplified for local testing)
spark = SparkSession.builder \
    .appName("PatientVitalsStream") \
    .config("spark.sql.streaming.checkpointLocation", "/tmp/spark-checkpoint") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")
print("Spark session created successfully")

# 3. Create some sample data first if file doesn't exist
import os
if not os.path.exists("heart_rate_stream.jsonl"):
    print("Creating sample data file...")
    with open("heart_rate_stream.jsonl", "w") as f:
        sample_data = [
            {"patient_id": 1001, "heart_rate": 72, "timestamp": "2025-08-17 10:00:00"},
            {"patient_id": 1002, "heart_rate": 85, "timestamp": "2025-08-17 10:00:01"},
            {"patient_id": 1003, "heart_rate": 68, "timestamp": "2025-08-17 10:00:02"}
        ]
        for record in sample_data:
            f.write(json.dumps(record) + "\n")

# 4. Read streaming data from JSONL file
try:
    print("Setting up file stream...")
    raw_df = spark.readStream \
        .format("text") \
        .option("path", "./heart_rate_stream.jsonl") \
        .load()
    
    print("Parsing JSON data...")
    # Parse JSON from file
    parsed_df = raw_df.select(from_json(col("value"), schema).alias("data")) \
        .select("data.*") \
        .filter(col("patient_id").isNotNull())  # Filter out malformed records
    
    # Add processing timestamp
    final_df = parsed_df.withColumn("processed_at", current_timestamp())
    
    print("Starting streaming query...")
    query = final_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime='3 seconds') \
        .start()
    
    print("Streaming query started successfully! Add data to heart_rate_stream.jsonl to see it appear.")
    print("Press Ctrl+C to stop.")
    query.awaitTermination()
    
except KeyboardInterrupt:
    print("\nStopping stream...")
    if 'query' in locals():
        query.stop()
    spark.stop()
except Exception as e:
    print(f"Streaming error: {e}")
    if 'query' in locals():
        query.stop()
    spark.stop()