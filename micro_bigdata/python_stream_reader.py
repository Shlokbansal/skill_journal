"""
Python-based Streaming Reader (Spark Alternative)
Demonstrates streaming concepts using pure Python
- Watches JSONL file for new records
- Processes data in micro-batches
- Handles malformed JSON gracefully
"""

import json
import time
import os
from datetime import datetime
from typing import Dict, List
from dataclasses import dataclass

@dataclass
class PatientVital:
    patient_id: int
    heart_rate: int
    timestamp: str
    processed_at: str = None

class StreamProcessor:
    def __init__(self, file_path: str = "heart_rate_stream.jsonl"):
        self.file_path = file_path
        self.last_position = 0
        self.processed_count = 0
        
    def read_new_records(self) -> List[PatientVital]:
        """Read new records from file since last position"""
        records = []
        
        if not os.path.exists(self.file_path):
            return records
            
        try:
            with open(self.file_path, 'r') as f:
                f.seek(self.last_position)
                
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                        
                    try:
                        data = json.loads(line)
                        vital = PatientVital(
                            patient_id=data.get('patient_id'),
                            heart_rate=data.get('heart_rate'),
                            timestamp=data.get('timestamp'),
                            processed_at=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        )
                        records.append(vital)
                    except json.JSONDecodeError as e:
                        print(f"Malformed JSON: {line} - Error: {e}")
                        
                self.last_position = f.tell()
                
        except Exception as e:
            print(f"Error reading file: {e}")
            
        return records
    
    def process_batch(self, records: List[PatientVital]):
        """Process a batch of records"""
        if not records:
            return
            
        print(f"\n--- Processing batch of {len(records)} records ---")
        for record in records:
            # Simulate processing logic
            status = "NORMAL" if 60 <= record.heart_rate <= 100 else "ALERT"
            print(f"Patient {record.patient_id}: HR={record.heart_rate} bpm [{status}] at {record.processed_at}")
            
        self.processed_count += len(records)
        
    def run(self, batch_interval: int = 3):
        """Run the streaming processor"""
        print(f"Starting streaming processor...")
        print(f"Watching file: {self.file_path}")
        print(f"Batch interval: {batch_interval} seconds")
        print("Press Ctrl+C to stop\n")
        
        try:
            while True:
                records = self.read_new_records()
                if records:
                    self.process_batch(records)
                else:
                    print(f"No new records. Total processed: {self.processed_count}")
                    
                time.sleep(batch_interval)
                
        except KeyboardInterrupt:
            print(f"\nStopping processor. Total records processed: {self.processed_count}")

def create_sample_data():
    """Create sample data for testing"""
    file_path = "heart_rate_stream.jsonl"
    
    if not os.path.exists(file_path):
        print("Creating sample data...")
        sample_data = [
            {"patient_id": 1001, "heart_rate": 72, "timestamp": "2025-08-17 10:00:00"},
            {"patient_id": 1002, "heart_rate": 85, "timestamp": "2025-08-17 10:00:01"},
            {"patient_id": 1003, "heart_rate": 68, "timestamp": "2025-08-17 10:00:02"},
        ]
        
        with open(file_path, "w") as f:
            for record in sample_data:
                f.write(json.dumps(record) + "\n")
        print(f"Created {len(sample_data)} sample records")

if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--demo":
        # Demo mode: process existing data once
        processor = StreamProcessor()
        records = processor.read_new_records()
        if records:
            processor.process_batch(records)
        else:
            print("No data found. Run kafka producer first.")
    else:
        # Normal streaming mode
        create_sample_data()
        processor = StreamProcessor()
        processor.run()