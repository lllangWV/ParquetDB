import itertools
import json
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

    data = general_utils.generate_pandas_data(n_rows=1000000)

    start_time = time.time()
    db.create(data)
    insert_time = time.time() - start_time
    del data

    start_time = time.time()
    data = db.read()
    read_time = time.time() - start_time
    del data

    benchmark_dict = {}
    new_data = general_utils.generate_pylist_data(n_rows=10, n_columns=110)

    start_time = time.time()
    db.create(new_data)

    benchmark_dict["create_new_schema_time"] = time.time() - start_time
    benchmark_dict["new_fields_added"] = 10
    benchmark_dict["new_rows_added"] = 10

    print("---")
    with open(os.path.join(save_dir, "schema_change_benchmark.json"), "w") as f:
        json.dump(benchmark_dict, f, indent=4)
