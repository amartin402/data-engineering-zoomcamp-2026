
"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns
columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the meter was engaged"
  - name: dropoff_datetime
    type: timestamp
    description: "When the meter was disengaged"
  - name: pickup_location_id
    type: integer
  - name: dropoff_location_id
    type: integer
  - name: fare_amount
    type: double
  - name: taxi_type
    type: string
  - name: payment_type
    type: integer

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python

import os
import json
import pandas as pd


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    # 1. Parse environment variables
    # We use .get() to avoid KeyErrors if running outside Bruin
    start_date = pd.to_datetime(os.environ.get("BRUIN_START_DATE", "2024-01-01"))
    end_date = pd.to_datetime(os.environ.get("BRUIN_END_DATE", "2024-02-01"))
    
    vars_dict = json.loads(os.environ.get("BRUIN_VARS", "{}"))
    taxi_types = vars_dict.get("taxi_types", ["yellow"])

    # 2. Generate months - Ensure we only get the start of each month
    months = pd.date_range(start=start_date, end=end_date, freq='MS')
    
    all_frames = []
    
    # 3. Fetch data
    for taxi in taxi_types:
        for month in months:
            year_str = month.strftime('%Y')
            month_str = month.strftime('%m')
            
            url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi}_tripdata_{year_str}-{month_str}.parquet"
            
            try:
                # Use columns parameter in read_parquet to only load what we need from disk
                # This significantly reduces memory usage!
                raw_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime', 'PULocationID', 'DOLocationID', 'fare_amount', 'payment_type'] 
                if taxi == 'green':
                    raw_cols = [c.replace('tpep', 'lpep') for c in raw_cols]

                df = pd.read_parquet(url, columns=raw_cols)
                
                # 4. Normalize
                rename_map = {
                    'tpep_pickup_datetime': 'pickup_datetime',
                    'tpep_dropoff_datetime': 'dropoff_datetime',
                    'lpep_pickup_datetime': 'pickup_datetime',
                    'lpep_dropoff_datetime': 'dropoff_datetime',
                    'PULocationID': 'pickup_location_id',
                    'DOLocationID': 'dropoff_location_id'
                }
                df = df.rename(columns=rename_map)
                df['taxi_type'] = taxi
                
                all_frames.append(df)
                print(f"Successfully loaded {taxi} {year_str}-{month_str}")
                
            except Exception as e:
                print(f"Skipping {taxi} {year_str}-{month_str}: {e}")

    if not all_frames:
        return pd.DataFrame()
        
    return pd.concat(all_frames, ignore_index=True)


