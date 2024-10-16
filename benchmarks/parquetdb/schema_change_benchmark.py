import json
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
from parquetdb.utils import general_utils

config.logging_config.loggers.timing.level='ERROR'
config.apply()


if __name__ == '__main__':
    save_dir=os.path.join(config.data_dir, 'benchmarks', 'parquetdb')
    os.makedirs(save_dir, exist_ok=True)

    benchmark_dir=os.path.join(save_dir, 'BenchmarkDB')
    dataset_dir=os.path.join(save_dir, 'parquetdb')

    if os.path.exists(dataset_dir):
        shutil.rmtree(dataset_dir)


    db = ParquetDB(dataset_name='parquetdb', dir=benchmark_dir)
    
    if db.dataset_exists():
        db.drop_dataset()  # Assuming there's a method to clear the database
    data=general_utils.generate_pandas_data(n_rows=1000000)
    
    start_time=time.time()
    db.create(data)
    insert_time = time.time() - start_time
    del data
    
    start_time=time.time()
    data = db.read()
    read_time = time.time() - start_time
    del data


    benchmark_dict={}
    new_data=general_utils.generate_pylist_data(n_rows=10, n_columns=110)
    
    start_time=time.time()
    db.create(new_data)
    
    benchmark_dict['create_new_schema_time']=time.time() - start_time
    benchmark_dict['new_fields_added']=10
    benchmark_dict['new_rows_added']=10
    
        
   
    print('---')
    with open(os.path.join(benchmark_dir, 'schema_change_benchmark.json'), 'w') as f:
        json.dump(benchmark_dict, f, indent=4)