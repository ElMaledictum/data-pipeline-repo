from sqlalchemy import create_engine
from prefect import flow, task
import pyarrow.parquet as pq
import pandas as pd
import time
import argparse

def init_args():
    parser = argparse.ArgumentParser(description='Arguments for integrating data to postgres db')
    parser.add_argument('--hostname', help="Database hostname")
    parser.add_argument('--port', help="Database port")
    parser.add_argument('--username', help="Database username")
    parser.add_argument('--password', help="Database password")
    parser.add_argument('--database', help="Database name")
    parser.add_argument('--table', help="Database table")
    parser.add_argument('--data_file', help="Parquet file containing the data")

    return parser.parse_args()


@task(log_prints=True, retries=3)
def ingest(data_file, user, password, host, port, db, table):
    parquet_file = pq.ParquetFile(data_file)
    engine = create_engine(f"postgresql://{user}:{password}@{host}:{port}/{db}")
    first_iteration = True

    for i in parquet_file.iter_batches(batch_size=100000):
        start_time = time.time()
        df = i.to_pandas()

        if first_iteration:
            pd.io.sql.get_schema(df, name=table, con=engine)
            df.to_sql(name=table, con=engine, if_exists='replace')
            first_iteration = False
        else:
            df.to_sql(name=table, con=engine, if_exists='append')

        end_time = time.time()
        print (f"Inserted {len(df)} rows of data to the database. Took {(end_time - start_time):.2f} seconds")


@flow(name="Ingest Flow")
def main ():
    args = init_args()
    host = args.hostname
    port = args.port
    user = args.username
    password = args.password
    db = args.database
    table = args.table
    data_file = args.data_file

    ingest(data_file, user, password, host, port, db, table)





if __name__ == "__main__":
    main()