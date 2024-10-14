import os
import shutil
import sys
import time
import random
import string
import itertools

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt


import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq
import pyarrow.fs as fs

from parquetdb import ParquetDB, config


config.logging_config.loggers.timing.level='ERROR'
config.apply()


def get_byte_size_of_type(data_type, n_char=100):
    if data_type == int:
        value = random.randint(0, 100000)
    elif data_type == float:
        value = random.uniform(0, 100000)
    elif data_type == str:
        value = ''.join(random.choices(string.ascii_letters, k=n_char))
    elif data_type == bool:
        value = random.choice([True, False])
    return sys.getsizeof(value)

def chunk_list_iter(lst, chunk_size):
    it = iter(lst)
    return iter(lambda: list(itertools.islice(it, chunk_size)), [])

# Function to generate random data for a given number of rows and columns
def generate_random_data(num_rows, num_cols):
    data_types = [int, float, str, bool]
    data = []
    for _ in range(num_rows):
        row = {}
        for col_index in range(num_cols):
            data_type = random.choice(data_types)
            col_name = f'col_{col_index}_{data_type.__name__}'
            if data_type == int:
                value = random.randint(0, 100000)
            elif data_type == float:
                value = random.uniform(0, 100000)
            elif data_type == str:
                value = ''.join(random.choices(string.ascii_letters, k=100))
            elif data_type == bool:
                value = random.choice([True, False])
            row[col_name] = value
        data.append(row)
    return data


def create_data(n_rows=100, n_columns=1000, data_type=int, n_char=100):
    
    if data_type == int:
        value = random.randint(0, 100000)
    elif data_type == float:
        value = random.uniform(0, 100000)
    elif data_type == str:
        value = ''.join(random.choices(string.ascii_letters, k=n_char))
    elif data_type == bool:
        value = random.choice([True, False])
        
    val=1000000
    data=[]
    for _ in range(n_rows):
        data.append({f'col_{i}':value for i in range(0,n_columns)})
    return data


def measure_memory(n_rows=100, n_columns=10, data_type=int):
    data=create_data(n_rows=n_rows, n_columns=n_columns, data_type=data_type)
    mem_before=pa.total_allocated_bytes()
    table=pa.Table.from_pylist(data)
    mem_after=pa.total_allocated_bytes()-mem_before
    return mem_after


# Function to benchmark the create operation
def benchmark_create(db, data, multiple_creates=False, batch_size=1000):
    start_time = time.time()
    if multiple_creates:
        # Split data into batches and create in multiple operations
        for i in range(0, len(data), batch_size):
            batch = data[i:i+batch_size]
            db.create(batch)
    else:
        # Single create operation
        db.create(data)
    end_time = time.time()
    return end_time - start_time

# Initialize the database


if __name__ == '__main__':
    save_dir=os.path.join(config.data_dir, 'benchmarks', 'parquetdb')
    benchmark_dir=os.path.join(save_dir, 'BenchmarkDB')
    
    dataset_dir=os.path.join(benchmark_dir, 'parquetdb')
    if os.path.exists(dataset_dir):
        shutil.rmtree(dataset_dir)

    db = ParquetDB(dataset_name='parquetdb', dir=benchmark_dir)

    # Parameters for the experiment
    num_columns_list = [1, 10, 100]
    num_rows_list = [1, 10, 100, 1000, 10000, 100000]
    multiple_creates_options = [False, True]
    batch_size_list  = [1000, 10000, 100000]
    data_types=[int,float,str,bool]
    n_chars=np.arange(1,10)
    # n_chars=[1,10,100]

    results = []
    # Run the experiment
    for num_cols in num_columns_list:
        for num_rows in num_rows_list:
            for data_type in data_types:
                if data_type==str:
                    n_chars_init=n_chars
                else:
                    n_chars_init=[1]
                
                for n_char in n_chars_init:
                    
                    byte_size=get_byte_size_of_type(data_type, n_char=n_char)
                    data = create_data(n_rows=num_rows, n_columns=num_cols, data_type=data_type, n_char=n_char)
                    # data = generate_random_data(num_rows, num_cols)
                    for multiple_creates in multiple_creates_options:
                        if multiple_creates:
                            batch_size_list_init=batch_size_list
                        else:
                            batch_size_list_init=[len(data)]
                        for batch_size in batch_size_list_init:
                            params_dict={
                                'num_cols': num_cols,
                                'num_rows': num_rows,
                                'n_chars': n_char,
                                'data_type': data_type,
                                'byte_size': byte_size,
                                'multiple_creates': multiple_creates,
                                'batch_size': batch_size,
                            }
                            if db.dataset_exists():
                                db.drop_dataset()  # Assuming there's a method to clear the database
                            time_taken = benchmark_create(db, data, multiple_creates=multiple_creates, batch_size=batch_size)
                            
                            params_dict.update({'time_taken': time_taken})
                            results.append(params_dict)
                            print(f'Columns: {num_cols}, Rows: {num_rows}, '
                                f'Multiple Creates: {multiple_creates}, Batch size: {batch_size}, '
                                f'Data Type: {data_type}, N Chars: {n_char}, '
                                f'Time Taken: {time_taken:.2f} seconds ')

    # Convert results to DataFrame
    results_df = pd.DataFrame(results)
    results_df.to_csv(os.path.join(save_dir, 'experiment_benchmark.csv'), index=False)
