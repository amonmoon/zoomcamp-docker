#!/usr/bin/env python
# coding: utf-8

import click
import pandas as pd
from sqlalchemy import create_engine, text

# Data types mapping
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

prefix = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/'


@click.command()
@click.option('--pg-user', default='root', help='Postgres username')
@click.option('--pg-password', default='root', help='Postgres password')
@click.option('--pg-host', default='localhost', help='Postgres host')
@click.option('--pg-port', default='5432', help='Postgres port')
@click.option('--pg-db', default='ny_taxi', help='Postgres database')
@click.option('--year', default=2021, type=int, help='Year of the taxi data file')
@click.option('--month', default=1, type=int, help='Month of the taxi data file')
@click.option('--chunk-size', default=100000, type=int, help='Chunk size for CSV ingestion')
@click.option('--target-table', default='yellow_taxi_data', help='Target table name')
def run(
    pg_user,
    pg_password,
    pg_host,
    pg_port,
    pg_db,
    year,
    month,
    chunk_size,
    target_table,
):
    engine = create_engine(f'postgresql+psycopg://{pg_user}:{pg_password}@{pg_host}:{pg_port}/{pg_db}')

    # Drop table if exists
    with engine.connect() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {target_table}"))
        conn.commit()

    # Read data in chunks
    df_iter = pd.read_csv(
        f'{prefix}yellow_tripdata_{year}-{month:02d}.csv.gz',
        dtype=dtype,
        parse_dates=parse_dates,
        iterator=True,
        chunksize=chunk_size
    )

    # Load chunks into database
    first = True
    for df_chunk in df_iter:
        if first:
            df_chunk.head(n=0).to_sql(
                name=target_table, 
                con=engine, 
                if_exists='replace'
            )
            first = False
        
        print(len(df_chunk))
        
        df_chunk.to_sql(
            name=target_table, 
            con=engine, 
            if_exists='append'
        )

    # Verify data
    query = "SELECT * FROM {target_table} LIMIT 10"
    df_sample = pd.read_sql(query, con=engine)
    print(df_sample.head())


if __name__ == "__main__":
    run()