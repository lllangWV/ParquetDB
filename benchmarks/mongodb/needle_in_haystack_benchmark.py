import os
import random
import time
from pymongo import MongoClient
import pandas as pd
from parquetdb import config

def generate_needle_in_haystack_data(n_rows=100, n_columns=100, value=-1):
    random_index = random.randint(0, n_rows-1)
    data=[]

    for j in range(n_rows):
        if j==random_index:
            data.append({f'col_{i}':value for i in range(n_columns)})
        else:
            data.append({f'col_{i}':random.randint(0, 100000) for i in range(n_columns)})
    return data

def remove_db(client, db_name):
    client.drop_database(db_name)

def benchmark_needle_in_haystack_serial(num_rows, client, db_name='benchmark', use_index=True):
    # Remove existing database to start fresh
    remove_db(client, db_name)
    
    # Generate data
    data = generate_needle_in_haystack_data(num_rows)
    
    # Start timing insertion
    start_time = time.time()
    
    # Insert data
    db = client[db_name]
    collection = db['test_collection']
    collection.insert_many(data)
    
    insert_time = time.time() - start_time
    data=None
    
    # Create an index on the 'id' field (index creation time not included in update time)
    if use_index:
        collection.create_index('col_0')
    #####################################################################################
    
    # Start timing reading
    start_time = time.time()
    
    # Execute read query
    # cursor = collection.find({})
    cursor = collection.find({"col_0": -1}, {'col_0': -1})
    results=list(cursor)  # Materialize the cursor
    assert results[0]['col_0'] == -1
    read_time = time.time() - start_time
    
    return insert_time, read_time



def benchmark_needle_in_haystack_with_index():
    save_dir = os.path.join(config.data_dir, 'benchmarks', 'mongodb')
    os.makedirs(save_dir, exist_ok=True)
    use_index=True
    
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
        
        insert_time, read_time = benchmark_needle_in_haystack_serial(num_rows, client, use_index=use_index)
        insertion_times.append(insert_time)
        reading_times.append(read_time)
        print(f'Insertion Time for {num_rows} rows: {insert_time:.4f} seconds')
        print(f'Reading Time for {num_rows} rows: {read_time:.4f} seconds')
        print('---')
        
    results['create_times'] = insertion_times
    results['read_times'] = reading_times
    results['n_rows'] = row_counts

    df = pd.DataFrame(results)
    df.to_csv(os.path.join(save_dir, 'mongodb_needle_in_haystack_with_index_benchmark.csv'), index=False)

    # Close the MongoDB connection
    client.close()


def benchmark_needle_in_haystack_without_index():
    save_dir = os.path.join(config.data_dir, 'benchmarks', 'mongodb')
    os.makedirs(save_dir, exist_ok=True)
    use_index=False
    
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
        
        insert_time, read_time = benchmark_needle_in_haystack_serial(num_rows, client, use_index=use_index)
        insertion_times.append(insert_time)
        reading_times.append(read_time)
        print(f'Insertion Time for {num_rows} rows: {insert_time:.4f} seconds')
        print(f'Reading Time for {num_rows} rows: {read_time:.4f} seconds')
        print('---')
        
    results['create_times'] = insertion_times
    results['read_times'] = reading_times
    results['n_rows'] = row_counts

    df = pd.DataFrame(results)
    df.to_csv(os.path.join(save_dir, 'mongodb_needle_in_haystack_without_index_benchmark.csv'), index=False)

    # Close the MongoDB connection
    client.close()
    

if __name__ == '__main__':
    
    
    benchmark_needle_in_haystack_with_index()
    benchmark_needle_in_haystack_without_index()