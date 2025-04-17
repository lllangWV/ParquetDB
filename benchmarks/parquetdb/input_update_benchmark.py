import gc
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

from parquetdb import NormalizeConfig, ParquetDB, config
from parquetdb.utils import general_utils

config.logging_config.loggers.timing.level = "ERROR"
config.apply()


if __name__ == "__main__":
    save_dir = os.path.join(config.data_dir, "benchmarks", "parquetdb")
    os.makedirs(save_dir, exist_ok=True)

    # Define the path to the database
    db_path = os.path.join(save_dir, "BenchmarkDB")

    # Remove the database if it exists
    if os.path.exists(db_path):
        shutil.rmtree(db_path)

    # Initialize the database
    db = ParquetDB(db_path=db_path)

    # Generate data
    data = general_utils.generate_pydict_data(n_rows=1000000, n_columns=100)

    start_time = time.time()
    db.create(data)

    insert_time = time.time() - start_time
    del data

    start_time = time.time()
    data = db.read()
    read_time = time.time() - start_time

    db.normalize()
    del data
    del db

    row_counts = [1, 10, 100, 1000, 10000, 100000, 1000000]
    n_columns = 100
    insertion_times = []
    reading_times = []
    update_times = []

    input_data_generator_dict = {
        "pylist": general_utils.generate_pylist_update_data,
        "pydict": general_utils.generate_pydict_update_data,
        "pandas": general_utils.generate_pandas_update_data,
        "table": general_utils.generate_table_update_data,
    }

    results = {
        "create_times": [],
        "read_times": [],
        "update_times": [],
        "input_data_type": [],
        "n_rows": [],
    }

    for num_rows in row_counts:
        for key, value in input_data_generator_dict.items():
            db = ParquetDB(db_path)
            print(f"Benchmarking {num_rows} rows. Using {key} input data...")
            update_func = value

            update_data = update_func(n_rows=num_rows, n_columns=n_columns)

            start_time = time.perf_counter()
            db.update(update_data, normalize_config=NormalizeConfig(batch_size=1200000))

            update_time = time.perf_counter() - start_time
            del update_data
            del db

            time.sleep(10)

            print(f"Insertion Time for {num_rows} rows: {insert_time:.4f} seconds")
            print(f"Reading Time for {num_rows} rows: {read_time:.4f} seconds")
            print(f"Updating Time for {num_rows} rows: {update_time:.4f} seconds")
            print("---")

            results["create_times"].append(insert_time)
            results["read_times"].append(read_time)
            results["update_times"].append(update_time)
            results["input_data_type"].append(key)
            results["n_rows"].append(num_rows)

    df = pd.DataFrame(results)
    df.to_csv(
        os.path.join(save_dir, "parquetdb_input_update_benchmark.csv"), index=False
    )
