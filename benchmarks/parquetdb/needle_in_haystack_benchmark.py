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



def generate_needle_in_haystack_data(n_rows=100, n_columns=100, value=-1):
    random_index = random.randint(0, n_rows-1)
    data=[]

    for j in range(n_rows):
        if j==random_index:
            data.append({f'col_{i}':value for i in range(n_columns)})
        else:
            data.append({f'col_{i}':random.randint(0, 100000) for i in range(n_columns)})
    return data



def benchmark_needle_in_haystack(num_rows):
    db = ParquetDB(dataset_name='parquetdb', dir=benchmark_dir)
    if db.dataset_exists():
        db.drop_dataset()  # Assuming there's a method to clear the database
    data=generate_needle_in_haystack_data(n_rows=num_rows)
    
    start_time=time.time()
    db.create(data)
    insert_time = time.time() - start_time
    data=None
    start_time=time.time()
    data = db.read(columns=['col_0'],filters=[(pc.field('col_0') == -1)]).combine_chunks()
    assert data['col_0'].to_pylist()[0]==-1
    read_time = time.time() - start_time
    return insert_time, read_time




if __name__ == '__main__':
    save_dir=os.path.join(config.data_dir, 'benchmarks', 'parquetdb')
    os.makedirs(save_dir, exist_ok=True)

    benchmark_dir=os.path.join(save_dir, 'BenchmarkDB')
    dataset_dir=os.path.join(benchmark_dir, 'parquetdb')

    if os.path.exists(dataset_dir):
        shutil.rmtree(dataset_dir)


    db = ParquetDB(dataset_name='parquetdb', dir=benchmark_dir)


    row_counts = [1, 10, 100, 1000, 10000, 100000, 1000000]
    insertion_times = []
    reading_times = []


    results={
        'create_times':[],
        'read_times':[],
        'n_rows':[]
    }
    for num_rows in row_counts:
        print(f'Benchmarking {num_rows} rows...')
        
        insert_time,read_time = benchmark_needle_in_haystack(num_rows)
        insertion_times.append(insert_time)
        reading_times.append(read_time)
        
        print(f'Insertion Time for {num_rows} rows: {insert_time:.4f} seconds')
        print(f'Reading Time for {num_rows} rows: {read_time:.4f} seconds')
        print('---')
        
    results['create_times']=insertion_times
    results['read_times']=reading_times
    results['n_rows']=row_counts

    df=pd.DataFrame(results)
    df.to_csv(os.path.join(save_dir, 'parquetdb_needle_in_haystack_benchmark.csv'), index=False)