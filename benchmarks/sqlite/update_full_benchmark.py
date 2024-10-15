import os
import random
import sqlite3
import time

import pandas as pd
from parquetdb import config

def generate_data(num_rows, n_columns=100):
    data = []
    for i in range(num_rows):
        # Include a unique identifier 'id' as the first column
        row = (i,) + tuple(random.randint(0, 1000000) for _ in range(n_columns - 1))
        data.append(row)
    return data

def remove_db_file(db_name):
    if os.path.exists(db_name):
        os.remove(db_name)

def benchmark_read_write_update(num_rows, db_name='benchmark.db'):
    # Remove existing database file to start fresh
    remove_db_file(db_name)
    
    # Generate data
    data = generate_data(num_rows)
    
    # Connect to SQLite database on disk
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    # Create table with 'id' as the primary key
    columns = ', '.join([f'col{i} INTEGER' for i in range(0, 100)])
    cursor.execute(f'CREATE TABLE IF NOT EXISTS test_table ({columns})')
    
    # Insert data
    placeholders = ', '.join('?' for _ in range(100))
    insert_query = f'INSERT INTO test_table VALUES ({placeholders})'
    conn.execute('PRAGMA synchronous = OFF')
    conn.execute('PRAGMA journal_mode = MEMORY')
    
    start_time = time.time()
    cursor.executemany(insert_query, data)
    conn.commit()
    insert_time = time.time() - start_time

    # Ensure data is written to disk
    conn.close()
    data = None  # Release memory
    
    #######################################################################
    
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()
    
    cursor.execute('CREATE INDEX idx_col0 ON test_table (col0)')
    conn.commit()
    # Ensure data is written to disk
    conn.close()

    #####################################################################
    # Reconnect to simulate fresh read from disk
    conn = sqlite3.connect(db_name)
    cursor = conn.cursor()

    # Measure read time
    start_time = time.time()
    cursor.execute('SELECT * FROM test_table')
    rows = cursor.fetchall()
    read_time = time.time() - start_time
    rows=None
    

    # Prepare update data (new values for existing IDs)
    update_data = generate_data(num_rows)
    update_values = []
    for row in update_data:
        # All columns except 'id', then 'id' for WHERE clause
        update_values.append(row[1:] + (row[0],))

   
    # cursor.execute('BEGIN TRANSACTION')
    # Construct update query to update columns col1 to col99
    update_query = f"UPDATE test_table SET " + ', '.join([f'col{i} = ?' for i in range(1, 100)]) + " WHERE col0 = ?"
    
     # Measure update time
    start_time = time.time()
    cursor.executemany(update_query, update_values)
    conn.commit()
    update_time = time.time() - start_time
    
    conn.close()
    update_data=None

    return insert_time, read_time, update_time

if __name__ == '__main__':
    save_dir = os.path.join(config.data_dir, 'benchmarks', 'sqlite')
    os.makedirs(save_dir, exist_ok=True)
    db_name = os.path.join(save_dir, 'benchmark.db')

    row_counts = [1, 10, 100, 1000, 10000, 100000, 1000000]
    insertion_times = []
    reading_times = []
    update_times = []

    results = {
        'create_times': [],
        'read_times': [],
        'update_times': [],
        'n_rows': []
    }

    for num_rows in row_counts:
        print(f'Benchmarking {num_rows} rows...')
        
        insert_time, read_time, update_time = benchmark_read_write_update(num_rows, db_name=db_name)
        insertion_times.append(insert_time)
        reading_times.append(read_time)
        update_times.append(update_time)
        
        print(f'Insertion Time for {num_rows} rows: {insert_time:.4f} seconds')
        print(f'Reading Time for {num_rows} rows: {read_time:.4f} seconds')
        print(f'Update Time for {num_rows} rows: {update_time:.4f} seconds')
        print('---')
        
    results['create_times'] = insertion_times
    results['read_times'] = reading_times
    results['update_times'] = update_times
    results['n_rows'] = row_counts

    df = pd.DataFrame(results)
    df.to_csv(os.path.join(save_dir, 'sqlite_update_benchmark.csv'), index=False)