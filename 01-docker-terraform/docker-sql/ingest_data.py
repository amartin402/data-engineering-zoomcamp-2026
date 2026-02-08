#!/usr/bin/env python
# coding: utf-8

import pandas as pd
from tqdm.auto import tqdm
from sqlalchemy import create_engine
import click

prefix='https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow'


dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime"
]

@click.command()
@click.option('--year', type=int, default=2021, help='Year of data')
@click.option('--month', type=int, default=1, help='Month of data')
@click.option('--chunksize', type=int, default=100000, help='Chunk size for reading CSV')
@click.option('--sql_table_name', type=str, default='yellow_taxi_data', help='SQL table name')
@click.option('--pg_user', type=str, default='root', help='PostgreSQL user')
@click.option('--pg_password', type=str, default='root', help='PostgreSQL password')
@click.option('--pg_host', type=str, default='localhost', help='PostgreSQL host')
@click.option('--pg_port', type=int, default=5432, help='PostgreSQL port')
@click.option('--pg_db', type=str, default='ny_taxi', help='PostgreSQL database')
def main(year, month, chunksize, sql_table_name, pg_user, pg_password, pg_host, pg_port, pg_db):
    url = f'{prefix}/yellow_tripdata_{year}-{month:02d}.csv.gz'
    
    click.echo(f"Starting data ingestion...")
    click.echo(f"URL: {url}")
    click.echo(f"Table: {sql_table_name}")

    engine = create_engine(f'postgresql://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    df_iter = pd.read_csv(
        url,
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunksize
    )


    first = True

    for df_chunk in tqdm(df_iter):

        if first:
            # Create table schema (no data)
            df_chunk.head(0).to_sql(
                name=sql_table_name,
                con=engine,
                if_exists="replace"
            )
            first = False
            print("Table created")

        # Insert chunk
        df_chunk.to_sql(
            name=sql_table_name,
            con=engine,
            if_exists="append"
        )

        print("Inserted:", len(df_chunk))

if __name__ == '__main__':
    main()
