import os
import random
import time
from pymongo import MongoClient
import pandas as pd
from parquetdb import config

def generate_data(num_rows, n_columns=100):
    data = []
    for _ in range(num_rows):
        doc = {f'col{i}': random.randint(0, 1000000) for i in range(n_columns)}
        data.append(doc)
    return data

def remove_db(client, db_name):
    client.drop_database(db_name)

def benchmark_read_write_serial(num_rows, client, db_name='benchmark'):
    # Remove existing database to start fresh
    remove_db(client, db_name)
    
    # Generate data
    data = generate_data(num_rows)
    
    # Start timing insertion
    start_time = time.time()
    
    # Insert data
    db = client[db_name]
    collection = db['test_collection']
    collection.insert_many(data)
    
    insert_time = time.time() - start_time
    data=None
    # Start timing reading
    start_time = time.time()
    
    # Execute read query
    cursor = collection.find({})
    list(cursor)  # Materialize the cursor
    
    read_time = time.time() - start_time
    
    return insert_time, read_time

if __name__ == '__main__':
    save_dir = os.path.join(config.data_dir, 'benchmarks', 'mongodb')
    os.makedirs(save_dir, exist_ok=True)
    
    # Connect to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    
    row_counts = [1, 10, 100, 1000, 10000, 100000, 1000000]
    insertion_times = []
    reading_times = []

    results = {
        'create_times': [],
        'read_times': [],
        'n_rows': []
    }

    for num_rows in row_counts:
        print(f'Benchmarking {num_rows} rows...')
        
        insert_time, read_time = benchmark_read_write_serial(num_rows, client)
        insertion_times.append(insert_time)
        reading_times.append(read_time)
        print(f'Insertion Time for {num_rows} rows: {insert_time:.4f} seconds')
        print(f'Reading Time for {num_rows} rows: {read_time:.4f} seconds')
        print('---')
        
    results['create_times'] = insertion_times
    results['read_times'] = reading_times
    results['n_rows'] = row_counts

    df = pd.DataFrame(results)
    df.to_csv(os.path.join(save_dir, 'mongodb_benchmark.csv'), index=False)

    # Close the MongoDB connection
    client.close()