# Big Data concepts with pandas (Spark syntax in comments for reference)
import pandas as pd

print("=== Big Data Processing Demo ===")
print("Using pandas to demonstrate Spark-like operations\n")

# Read CSV (equivalent to: spark.read.csv("patients.csv", header=True, inferSchema=True))
df = pd.read_csv("patients.csv")

print("Dataset loaded:")
print(df)
print(f"\nDataset shape: {df.shape}")

# SQL-style aggregation (equivalent to: df.groupBy("gender").avg("heart_rate").show())
print("\n=== Spark-style Aggregation ===")
avg_by_gender = df.groupby("gender")["heart_rate"].mean()
print("Average heart rate by gender:")
for gender, avg_hr in avg_by_gender.items():
    print(f"{gender}: {avg_hr:.1f} bpm")

# Additional analytics for healthcare use case
print("\n=== Healthcare Analytics ===")
print("Heart rate statistics by gender:")
stats = df.groupby("gender")["heart_rate"].agg(['count', 'mean', 'min', 'max', 'std']).round(1)
print(stats)

# Identify patients with abnormal heart rates (demo of filtering)
print("\n=== Clinical Insights ===")
normal_range = (60, 85)
abnormal = df[(df["heart_rate"] < normal_range[0]) | (df["heart_rate"] > normal_range[1])]
if len(abnormal) > 0:
    print("Patients with heart rates outside normal range (60-85 bpm):")
    print(abnormal)
else:
    print("All patients have heart rates within normal range (60-85 bpm)")

print("\n=== Next Steps for Production ===")
print("- Scale to millions of records with PySpark")
print("- Add real-time streaming with Kafka")
print("- Deploy on cloud clusters (EMR, Databricks)")
