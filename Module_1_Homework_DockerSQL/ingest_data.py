#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
import requests
import shutil
import gzip
from sqlalchemy import create_engine


def main(params):
    # Extract parameters
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_name1 = params.table_name1
    url1 = params.url1
    table_name2 = params.table_name2
    url2 = params.url2
    gz_filename = 'trips.csv.gz'
    file_name2 = 'zones.csv'
    file_name1 = 'trips.csv'

    # Function to download the file if it doesn't exist
    def download_file(url, local_filename):
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print(f"Downloaded {local_filename}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {e}")
            return False
        return True

    # Function to unzip the .gz file
    def unzip_file(gz_filename, output_filename):
        try:
            # Open the gzipped file and the output file
            with gzip.open(gz_filename, 'rb') as f_in:
                with open(output_filename, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        
            # If no exceptions occur, print a success message and return True
            print(f"Unzipped {gz_filename} to {output_filename}")
            return True
        
        except Exception as e:
         # If an error occurs, print the error and return False
            print(f"Error unzipping {gz_filename}: {e}")
            return False


    # Download the .gz file
    if download_file(url1, gz_filename):
        # Unzip the downloaded .gz file
        if unzip_file(gz_filename, file_name1):
            # Step 3: Now read the CSV file into pandas
            df1 = pd.read_csv(file_name1, low_memory=False)

    # Download the second file
    if download_file(url2, file_name2):
         # Read the downloaded the second file
        df2 = pd.read_csv(file_name2)

    # Create the PostgreSQL engine
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    # Function to generate batch iterator
    def batch_iterator(df, batch_size):
        total_rows = len(df)
        for start in range(0, total_rows, batch_size):
            end = min(start + batch_size, total_rows)
            yield df.iloc[start:end]

     # Function to insert data in batches
    def insert_in_batches(df, table_name, engine, batch_size):
        # Create only the table structure in PostgreSQL (without data)
        df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')

        # Create a batch iterator
        df_iter = batch_iterator(df, batch_size)

        for batch_df in df_iter:
            # Insert each batch into the db table
            batch_df.to_sql(table_name, engine, if_exists='append', index=True)
            print(f'Inserted batch of {len(batch_df)} rows into {table_name}')

    # Insert the first dataframe into the first table
    print(f'Ingesting data from {file_name1} into table {table_name1}...')
    insert_in_batches(df1, table_name1, engine, batch_size=100000)

    # Insert the second dataframe into the second table
    print(f'Ingesting data from {file_name2} into table {table_name2}...')
    insert_in_batches(df2, table_name2, engine, batch_size=100000)

    print('All batches inserted successfully')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data into PostgreSQL')

    parser.add_argument('user', help='user name for postgres')
    parser.add_argument('password', help='password for postgres')
    parser.add_argument('host', help='hostname for postgres')
    parser.add_argument('port', type=int, help='port for postgres')
    parser.add_argument('db', help='database name for postgres')
    parser.add_argument('table_name1', help='first table name for postgres where results will be written')
    parser.add_argument('url1', help='first URL of the CSV file')
    parser.add_argument('table_name2', help='second table name for postgres where results will be written')
    parser.add_argument('url2', help='second URL of the CSV file')

    args = parser.parse_args()

    main(args)
