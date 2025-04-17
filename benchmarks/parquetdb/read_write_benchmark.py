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
from parquetdb.utils import general_utils

config.logging_config.loggers.timing.level = "ERROR"
config.apply()


def benchmark_read_write(num_rows, db_path):
    # Remove the database if it exists
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # Initialize the database
    db = ParquetDB(db_path)

    data = general_utils.generate_pylist_data(n_rows=num_rows)

    start_time = time.time()
    db.create(data)
    insert_time = time.time() - start_time
    data = None

    start_time = time.time()
    data = db.read()
    read_time = time.time() - start_time

    return insert_time, read_time


if __name__ == "__main__":
    save_dir = os.path.join(config.data_dir, "benchmarks", "parquetdb")
    os.makedirs(save_dir, exist_ok=True)

    # Define the path to the database
    db_path = os.path.join(save_dir, "BenchmarkDB")

    row_counts = [1, 10, 100, 1000, 10000, 100000, 1000000]
    insertion_times = []
    reading_times = []

    results = {"create_times": [], "read_times": [], "n_rows": []}
    for num_rows in row_counts:
        print(f"Benchmarking {num_rows} rows...")

        insert_time, read_time = benchmark_read_write(num_rows, db_path)
        insertion_times.append(insert_time)
        reading_times.append(read_time)

        time.sleep(5)

        print(f"Insertion Time for {num_rows} rows: {insert_time:.4f} seconds")
        print(f"Reading Time for {num_rows} rows: {read_time:.4f} seconds")
        print("---")

    results["create_times"] = insertion_times
    results["read_times"] = reading_times
    results["n_rows"] = row_counts

    df = pd.DataFrame(results)
    df.to_csv(os.path.join(save_dir, "parquetdb_benchmark.csv"), index=False)
