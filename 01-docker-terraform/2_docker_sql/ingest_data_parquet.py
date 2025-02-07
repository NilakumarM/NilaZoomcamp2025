import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import create_engine
import time
import argparse
import os
import requests  # Import requests for downloading the file


def download_parquet_file(url, output_path):
    """
    Downloads a Parquet file from the given URL and saves it locally.
    """
    try:
        print(f"Downloading data from {url}...")
        response = requests.get(url, stream=True)
        response.raise_for_status()  # Raise an error for bad status codes

        # Write the file to disk in chunks
        with open(output_path, "wb") as file:
            for chunk in response.iter_content(chunk_size=8192):
                file.write(chunk)

        print("Download completed successfully.")
    except requests.RequestException as e:
        print(f"Error downloading file: {e}")
        exit(1)


def main(params):
    # Extract parameters from command-line arguments
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name = params.table_name
    url = params.url
    parquet_file = "output.parquet"  # Keep consistent file name

    # Download the Parquet file
    download_parquet_file(url, parquet_file)

    # Construct database connection string dynamically
    database_uri = f"postgresql://{user}:{password}@{host}:{port}/{db}"

    # Initialize database engine
    try:
        engine = create_engine(database_uri)
        print("Database connection successful.")
    except Exception as e:
        print(f"Database connection failed: {e}")
        exit(1)

    # Load Parquet file
    print("Reading Parquet file...")
    parquet_file = pq.ParquetFile(parquet_file)

    # Process and insert data in chunks
    batch_size = 100000
    df_iter = parquet_file.iter_batches(batch_size=batch_size)

    while True:
        try:
            t_start = time.time()

            # Convert to Pandas DataFrame
            df = next(df_iter).to_pandas()

            # Convert datetime columns safely
            datetime_cols = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
            for col in datetime_cols:
                if col in df.columns:
                    df[col] = pd.to_datetime(df[col], errors="coerce")

            # Insert batch into the database
            df.to_sql(name=table_name, con=engine, if_exists="append", index=False)

            t_end = time.time()
            print(f"Inserted {len(df)} rows, took {t_end - t_start:.3f} seconds")

        except StopIteration:
            print("Finished reading and inserting all chunks.")
            break


if __name__ == "__main__":
    # Parse command-line arguments
    parser = argparse.ArgumentParser(description="Ingest Parquet data into Postgres")

    parser.add_argument("--user", required=True, help="PostgreSQL username")
    parser.add_argument("--password", required=True, help="PostgreSQL password")
    parser.add_argument("--host", required=True, help="PostgreSQL host")
    parser.add_argument("--port", required=True, help="PostgreSQL port")
    parser.add_argument("--db", required=True, help="PostgreSQL database name")
    parser.add_argument("--table_name", required=True, help="Table name for data insertion")
    parser.add_argument("--url", required=True, help="URL of the Parquet file")

    args = parser.parse_args()
    main(args)
