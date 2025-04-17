import itertools
import os
import random
import shutil
import string
import sys
import time

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.fs as fs
import pyarrow.parquet as pq

from parquetdb import ParquetDB, config
from parquetdb.core.parquetdb import NormalizeConfig

config.logging_config.loggers.timing.level = "ERROR"
config.apply()


def generate_data(n_rows=100, n_columns=100):
    data = []
    for _ in range(n_rows):
        data.append(
            {f"col_{i}": random.randint(0, 100000) for i in range(0, n_columns)}
        )

    return data


def generate_update_data(n_rows=100, n_columns=100):
    data = []
    for i in range(n_rows):
        row = {"id": i}  # Unique identifier for each row
        row.update({f"col_{j}": random.randint(0, 100000) for j in range(n_columns)})
        data.append(row)
    return data


def benchmark_read_write_update(num_rows, db_path):
    # Create the database if it doesn't exist
    if not os.path.exists(db_path):
        os.makedirs(db_path)

    db = ParquetDB(db_path)

    # Generate the data
    data = generate_data(n_rows=num_rows)

    start_time = time.time()
    db.create(data)
    insert_time = time.time() - start_time

    del data

    start_time = time.time()
    data = db.read()
    read_time = time.time() - start_time

    del data

    update_data = generate_update_data(n_rows=num_rows)

    start_time = time.time()
    db.update(
        update_data,
        normalize_config=NormalizeConfig(
            batch_size=50000,
            batch_readahead=16,
            fragment_readahead=4,
        ),
    )
    update_time = time.time() - start_time

    update_data = None
    return insert_time, read_time, update_time


if __name__ == "__main__":
    save_dir = os.path.join(config.data_dir, "benchmarks", "parquetdb")
    os.makedirs(save_dir, exist_ok=True)

    # Define the path to the database
    db_path = os.path.join(save_dir, "BenchmarkDB")

    # Remove the database if it exists
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # Initialize the database
    db = ParquetDB(db_path)

    # Define the row counts to benchmark
    row_counts = [1, 10, 100, 1000, 10000, 100000, 1000000]
    insertion_times = []
    reading_times = []
    update_times = []

    results = {"create_times": [], "read_times": [], "update_times": [], "n_rows": []}
    for num_rows in row_counts:
        print(f"Benchmarking {num_rows} rows...")

        insert_time, read_time, update_time = benchmark_read_write_update(
            num_rows, db_path
        )
        insertion_times.append(insert_time)
        reading_times.append(read_time)
        update_times.append(update_time)

        print(f"Insertion Time for {num_rows} rows: {insert_time:.4f} seconds")
        print(f"Reading Time for {num_rows} rows: {read_time:.4f} seconds")
        print(f"Updating Time for {num_rows} rows: {update_time:.4f} seconds")
        print("---")

    results["create_times"] = insertion_times
    results["read_times"] = reading_times
    results["update_times"] = update_times
    results["n_rows"] = row_counts

    df = pd.DataFrame(results)
    df.to_csv(
        os.path.join(save_dir, "parquetdb_update_full_benchmark.csv"), index=False
    )
