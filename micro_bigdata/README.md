# Intro to Spark for Big Data in Health Tech

**Date:** 2025-08-11

## Goal
Practice reading large datasets and performing aggregations using PySpark to simulate healthcare analytics.

## Code Summary
- Created a SparkSession
- Read `patients.csv`
- Grouped by gender to calculate average heart rate
- Printed summary statistics

## Health Tech Relevance
- Wearable companies (Oura, Fitbit) and health AI companies (Tempus) use Spark to process millions of records daily.
- Useful when datasets are too large for pandas or memory-bound tools.

## Next Steps
- Connect Spark to streaming data with Kafka
- Deploy analytics to cloud-based clusters (AWS EMR, Databricks)
