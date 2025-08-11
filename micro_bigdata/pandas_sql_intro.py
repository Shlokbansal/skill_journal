import pandas as pd

# Read CSV data (equivalent to Spark df.read.csv)
df = pd.read_csv("patients.csv")

print("Dataset:")
print(df)
print("\nDataset info:")
print(df.info())

# Example aggregation: avg heart rate by gender (equivalent to Spark SQL)
avg_by_gender = df.groupby("gender")["heart_rate"].mean()
print("\nAverage heart rate by gender:")
print(avg_by_gender)

# SQL-like operations with pandas
print("\nSQL-like aggregation:")
result = df.groupby("gender").agg({
    'heart_rate': ['mean', 'count', 'min', 'max']
}).round(2)
print(result)