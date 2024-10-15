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

def generate_data(n_rows=100, n_columns=100):
    data=[]
    for _ in range(n_rows):
        data.append({f'col_{i}':random.randint(0, 100000) for i in range(0,n_columns)})
    
    return data


def generate_update_data(n_rows=100, n_columns=100):
    data = []
    for i in range(n_rows):
        row = {'id': i}  # Unique identifier for each row
        row.update({f'col_{j}': random.randint(0, 100000) for j in range(n_columns)})
        data.append(row)
    return data

def generate_pyarrow_data(num_columns=100, num_rows=1000, min_value=0, max_value=100000):
    data = {}
    ids=[]
    for i in range(num_columns):
        column_name = f"column_{i+1}"
        data[column_name] = [random.randint(min_value, max_value) for _ in range(num_rows)]
        ids.append(i)
    return data


def generate_pyarrow_update_data(num_columns=100, num_rows=1000, min_value=0, max_value=100):
    data = {}
    ids=[]
    for i in range(num_columns):
        column_name = f"column_{i+1}"
        data[column_name] = [random.randint(min_value, max_value) for _ in range(num_rows)]
        ids.append(i)
    data['id'] = ids
    return data


def benchmark_read_write_update(num_rows):
    
    update_data=generate_update_data(n_rows=num_rows)
    update_data=db._construct_table(update_data)
    start_time = time.time()
    db.update(update_data,
              )
    update_time = time.time() - start_time
    
    del update_data
    return update_time




if __name__ == '__main__':
    save_dir=os.path.join(config.data_dir, 'benchmarks', 'parquetdb')
    os.makedirs(save_dir, exist_ok=True)

    benchmark_dir=os.path.join(save_dir, 'BenchmarkDB')
    dataset_dir=os.path.join(benchmark_dir, 'parquetdb')

    if os.path.exists(dataset_dir):
        shutil.rmtree(dataset_dir)


    db = ParquetDB(dataset_name='parquetdb', dir=benchmark_dir)
    if db.dataset_exists():
        db.drop_dataset()  # Assuming there's a method to clear the database
    data=generate_data(n_rows=1000000)
    
    start_time=time.time()
    db.create(data)
    insert_time = time.time() - start_time
    del data
    
    start_time=time.time()
    data = db.read()
    read_time = time.time() - start_time
    del data


    row_counts = [1, 10, 100, 1000, 10000, 100000, 1000000]
    insertion_times = []
    reading_times = []
    update_times=[]
    
    
    db.normalize(
        
    )


    results={
        'create_times':[],
        'read_times':[],
        'update_times':[],
        'n_rows':[]
    }
    for num_rows in row_counts:
        print(f'Benchmarking {num_rows} rows...')
        
        update_time = benchmark_read_write_update(num_rows)
        insertion_times.append(insert_time)
        reading_times.append(read_time)
        update_times.append(update_time)
        
        print(f'Insertion Time for {num_rows} rows: {insert_time:.4f} seconds')
        print(f'Reading Time for {num_rows} rows: {read_time:.4f} seconds')
        print(f"Updating Time for {num_rows} rows: {update_time:.4f} seconds")
        print('---')
        
    results['create_times']=insertion_times
    results['read_times']=reading_times
    results['update_times']=update_times
    results['n_rows']=row_counts

    df=pd.DataFrame(results)
    df.to_csv(os.path.join(save_dir, 'parquetdb_update_into_constant_rows_table_input_benchmark.csv'), index=False)