import sys
import pandas as pd

print("arguments:", sys.argv)

df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
print(df.head())

month = sys.argv[1]

print(f"Hello from pipeline.py, month: {month}")