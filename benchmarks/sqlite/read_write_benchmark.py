import os
import random
import sqlite3
import string
import time

import pandas as pd
from parquetdb import config

def generate_data(num_rows, n_columns=100):
    data = []
    for _ in range(num_rows):
        row = tuple(random.randint(0, 1000000) for _ in range(n_columns))
        data.append(row)
    return data

def remove_db_file(db_name):
    if os.path.exists(db_name):
        os.remove(db_name)


def benchmark_read_write(num_rows, db_name='benchmark.db'):
    # Remove existing database file to start fresh
    remove_db_file(db_name)
    
    # Generate data
    data = generate_data(num_rows)
    
    # Connect to SQLite database on disk
    
    
    start_time=time.time()
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create table and insert data (without timing, focusing on read performance)
    columns = ', '.join(f'col{i} INTEGER' for i in range(1, 101))
    cursor.execute(f'CREATE TABLE IF NOT EXISTS test_table ({columns})')
    placeholders = ', '.join('?' for _ in range(100))
    insert_query = f'INSERT INTO test_table VALUES ({placeholders})'
    conn.execute('PRAGMA synchronous = OFF')
    conn.execute('PRAGMA journal_mode = MEMORY')
    cursor.executemany(insert_query, data)
    conn.commit()
    # Ensure data is written to disk
    conn.close()
    insert_time=time.time()-start_time
    
    data=None
    
    # Reconnect to simulate fresh read from disk
    start_time=time.time()
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Execute read query
    cursor.execute('SELECT * FROM test_table')
    rows = cursor.fetchall()
    # print(rows)
    # Close the connection
    conn.close()
    read_time=time.time()-start_time
    return insert_time, read_time



if __name__ == '__main__':
    save_dir=os.path.join(config.data_dir, 'benchmarks', 'sqlite')
    os.makedirs(save_dir, exist_ok=True)
    db_name=os.path.join(save_dir, 'benchmark.db')

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
        
        insert_time,read_time = benchmark_read_write(num_rows, db_name=db_name)
        insertion_times.append(insert_time)
        reading_times.append(read_time)
        print(f'Insertion Time for {num_rows} rows: {insert_time:.4f} seconds')
        print(f'Reading Time for {num_rows} rows: {read_time:.4f} seconds')
        print('---')
        
    results['create_times']=insertion_times
    results['read_times']=reading_times
    results['n_rows']=row_counts

    df=pd.DataFrame(results)
    df.to_csv(os.path.join(save_dir, 'sqlite_benchmark.csv'), index=False)