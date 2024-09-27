from glob import glob
import json
import os
import random
import shutil
import tempfile
import time

import matplotlib as mpl
mpl.use('Agg')
import matplotlib.pyplot as plt

import pandas as pd
from typing import List

# Assuming ParquetDB is in the module parquetdb
from parquetdb import ParquetDB




def benchmark_read_performance():
    # Create a temporary directory for the database
    temp_dir = tempfile.mkdtemp()
    save_dir='C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/ParquetDB'
    # os.makedirs(save_dir, exist_ok=True)
    db = ParquetDB(db_path=save_dir)

    # Define dataset sizes to test
    dataset_sizes = [1000, 5000, 10000, 50000, 100000]  # Adjust sizes as needed
    read_times = []
    write_times = []

    materials_dir="C:/Users/lllang/Desktop/Current_Projects/ParquetDB/data/raw/materials_data"

    files=glob(os.path.join(materials_dir,'*.json'))
    print(files)
    materials_data=[]
    for file in files:
        with open(file, 'r') as f:
            materials_data.append(json.load(f))

    try:
        for size in dataset_sizes:
            print(f"Testing with dataset size: {size}")

            # Generate test data
            test_data = random.choices(materials_data, k=size)

            # test_data = [{'value': f"value_{i}", 'number': i * 2} for i in range(size)]

            # Create the table
            table_name = f"test_table_{size}"
            start_time = time.perf_counter()
            db.create(data=test_data, table_name=table_name)
            end_time = time.perf_counter()
            write_time = end_time - start_time
            write_times.append(write_time)

            print(f"Write time: {write_time:.4f} seconds")
            # Measure read time
            start_time = time.perf_counter()
            table = db.read(table_name=table_name, output_format='table')
            n_columns = table.shape[1]
            end_time = time.perf_counter()
            elapsed_time = end_time - start_time
            read_times.append(elapsed_time)

            print(f"Read time for {size} entries: {elapsed_time:.4f} seconds")
            print(f"Number of columns: {n_columns}")

            # Optionally verify the data (commented out to save time)
            # df = data.to_pandas()
            # assert len(df) == size, f"Data length mismatch: expected {size}, got {len(df)}"

            # Clean up by dropping the table (optional if disk space is limited)
            # db.drop_table(table_name)

    finally:
        pass
        # Remove the temporary directory after the test
        # shutil.rmtree(temp_dir)

    # Assuming dataset_sizes, read_times, write_times, and n_columns are already defined
    fig, ax1 = plt.subplots(figsize=(10, 6))

    # Plotting read times on the first axis
    ax1.plot(dataset_sizes, read_times, marker='o', label='Read Time (seconds)', color='b')
    ax1.set_xlabel('Number of Entries')
    ax1.set_ylabel('Read Time (seconds)', color='b')
    ax1.tick_params(axis='y', labelcolor='b')

    # Creating the second y-axis for write times
    ax2 = ax1.twinx()
    ax2.plot(dataset_sizes, write_times, marker='x', label='Write Time (seconds)', color='r')
    ax2.set_ylabel('Write Time (seconds)', color='r')
    ax2.tick_params(axis='y', labelcolor='r')

    # Adding a title
    plt.title(f'ParquetDB Read and Write Performance | Number of columns {n_columns}')

    # Adding grid for clarity
    ax1.grid(True)

    # Saving the plot
    plt.savefig(os.path.join('data', 'read_write_performance.png'))


    # Optionally, print the results in a table format
    print("\nBenchmark Results:")
    for size, time_taken in zip(dataset_sizes, read_times):
        print(f"{size} entries: {time_taken:.4f} seconds")

if __name__ == '__main__':
    benchmark_read_performance()
